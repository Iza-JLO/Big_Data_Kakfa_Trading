"""
Backtesting del algoritmo de cruce de medias sobre velas historicas.

Usa EXACTAMENTE la misma clase CruceMedias que el algoritmo en vivo.

Fuentes de velas (elige una):
    --csv ruta.csv    CSV con columnas: inicio, close (y opcionalmente symbol, open, high, low)
    --kafka           lee el topico "velas" completo (por ejemplo, tras correr el simulador)
    --demo N          genera N velas aleatorias, solo para probar que el script funciona

Ejemplos:
    python backtesting.py --demo 3000
    python backtesting.py --kafka --spread-pips 1.0
    python backtesting.py --csv eurusd_1m.csv --rapida 5 --lenta 20

Simplificaciones (conviene mencionarlas en el reporte):
    - Se opera siempre con 1 posicion: BUY abre largo (cierra corto), SELL abre corto (cierra largo).
    - Cada operacion se ejecuta al cierre de la vela que genera la senal (precio_referencia).
    - Las velas son de precio medio, asi que el costo se aproxima con --spread-pips por operacion.
    - Un pip = 0.0001 (valido para EURUSD y GBPUSD).
"""
import argparse
import csv
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from algoritmo_trading import CruceMedias, BOOTSTRAP, TOPIC_VELAS

PIP = 0.0001


# ---------------------------------------------------------------- carga de velas
def cargar_csv(ruta, simbolo):
    velas = []
    with open(ruta, newline="", encoding="utf-8") as f:
        for fila in csv.DictReader(f):
            if "inicio" not in fila or "close" not in fila:
                raise ValueError("El CSV necesita al menos las columnas 'inicio' y 'close'")
            fila.setdefault("symbol", simbolo)
            if not fila["symbol"]:
                fila["symbol"] = simbolo
            velas.append(fila)
    return velas


def cargar_kafka():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        TOPIC_VELAS,
        bootstrap_servers=BOOTSTRAP,
        group_id=f"backtest-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,  # termina cuando ya no llegan mas velas
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    velas = [m.value for m in consumer]
    consumer.close()
    return velas


def generar_demo(n, simbolo, semilla=42):
    rnd = random.Random(semilla)
    precio = 1.1600
    t = datetime(2026, 1, 5, 0, 0, tzinfo=timezone.utc)
    velas = []
    for _ in range(n):
        abre = precio
        precio += rnd.gauss(0, 0.00008)
        velas.append({
            "symbol": simbolo, "inicio": t.isoformat(),
            "open": abre, "high": max(abre, precio), "low": min(abre, precio),
            "close": precio, "num_ticks": 30,
        })
        t += timedelta(minutes=1)
    return velas


def limpiar(velas, simbolo):
    """Filtra por simbolo, quita duplicados (symbol, inicio) y ordena por tiempo."""
    unicas = {}
    for v in velas:
        if v.get("symbol") == simbolo:
            unicas[v["inicio"]] = v
    return [unicas[k] for k in sorted(unicas)]


# ------------------------------------------------------------------- simulacion
def simular(velas, estrategia, spread_pips):
    trades = []
    posicion = 0  # +1 largo, -1 corto, 0 fuera
    entrada = entrada_inicio = None

    def cerrar(precio_salida, inicio_salida):
        direccion = posicion
        pips = direccion * (precio_salida - entrada) / PIP - spread_pips
        trades.append({
            "direccion": "LARGO" if direccion == 1 else "CORTO",
            "entrada_inicio": entrada_inicio, "entrada": entrada,
            "salida_inicio": inicio_salida, "salida": precio_salida,
            "pips": round(pips, 2),
        })

    for v in velas:
        senal = estrategia.on_vela(v)
        if senal is None:
            continue
        nueva = 1 if senal["signal"] == "BUY" else -1
        if nueva == posicion:
            continue
        precio = senal["precio_referencia"]
        if posicion != 0:
            cerrar(precio, v["inicio"])
        posicion, entrada, entrada_inicio = nueva, precio, v["inicio"]

    if posicion != 0 and velas:  # cierra la posicion abierta al final del periodo
        cerrar(float(velas[-1]["close"]), velas[-1]["inicio"])
    return trades


def metricas(trades):
    if not trades:
        return None
    pips = [t["pips"] for t in trades]
    ganancias = sum(p for p in pips if p > 0)
    perdidas = -sum(p for p in pips if p < 0)
    acumulado, pico, max_dd = 0.0, 0.0, 0.0
    for p in pips:
        acumulado += p
        pico = max(pico, acumulado)
        max_dd = max(max_dd, pico - acumulado)
    return {
        "operaciones": len(pips),
        "ganadoras": sum(1 for p in pips if p > 0),
        "porcentaje_acierto": round(100 * sum(1 for p in pips if p > 0) / len(pips), 1),
        "pips_totales": round(sum(pips), 1),
        "pips_promedio_por_operacion": round(sum(pips) / len(pips), 2),
        "factor_de_beneficio": round(ganancias / perdidas, 2) if perdidas > 0 else None,
        "max_drawdown_pips": round(max_dd, 1),
    }


def main():
    parser = argparse.ArgumentParser()
    fuente = parser.add_mutually_exclusive_group(required=True)
    fuente.add_argument("--csv")
    fuente.add_argument("--kafka", action="store_true")
    fuente.add_argument("--demo", type=int, metavar="N")
    parser.add_argument("--simbolo", default="EURUSD")
    parser.add_argument("--rapida", type=int, default=5)
    parser.add_argument("--lenta", type=int, default=20)
    parser.add_argument("--spread-pips", type=float, default=1.0)
    parser.add_argument("--guardar-trades", metavar="RUTA_CSV")
    args = parser.parse_args()

    if args.csv:
        crudas = cargar_csv(args.csv, args.simbolo)
    elif args.kafka:
        crudas = cargar_kafka()
    else:
        crudas = generar_demo(args.demo, args.simbolo)

    velas = limpiar(crudas, args.simbolo)
    if not velas:
        print(f"No hay velas de {args.simbolo}. Revisa la fuente de datos.")
        return
    print(f"Velas: {len(velas)}  ({velas[0]['inicio']}  a  {velas[-1]['inicio']})")
    print(f"Estrategia: cruce SMA {args.rapida}/{args.lenta}, spread {args.spread_pips} pips por operacion")

    trades = simular(velas, CruceMedias(args.rapida, args.lenta), args.spread_pips)
    resultado = metricas(trades)
    if resultado is None:
        print("No hubo operaciones (pocas velas o ningun cruce).")
        return
    print("\nResultados:")
    for k, v in resultado.items():
        print(f"  {k}: {v}")

    if args.guardar_trades:
        with open(args.guardar_trades, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(trades[0].keys()))
            w.writeheader()
            w.writerows(trades)
        print(f"\nOperaciones guardadas en {args.guardar_trades}")


if __name__ == "__main__":
    main()
