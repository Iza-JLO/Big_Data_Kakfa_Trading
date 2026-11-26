"""
Simulador de ticks para probar el generador de velas SIN necesitar IB.

Publica en el tópico "ticks" eventos con el mismo formato que el productor real:
    {symbol, bid, ask, timestamp}

Los timestamps van hacia atrás en el tiempo y avanzan rápido, así que en pocos
segundos se generan varios minutos de ticks y Spark puede cerrar velas enseguida.

Uso (desde la carpeta trading_plataforma/producer, con Kafka corriendo):
    python simulador_ticks.py                 # 10 minutos de ticks simulados
    python simulador_ticks.py --minutos 5
    python simulador_ticks.py --vivo          # ritmo real, 1 tick por segundo aprox.
"""
import argparse
import random
import time
from datetime import datetime, timedelta, timezone

from kafka_productor import EventProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutos", type=int, default=10,
                        help="minutos de mercado simulado (modo rápido)")
    parser.add_argument("--simbolo", default="EURUSD")
    parser.add_argument("--precio", type=float, default=1.1600,
                        help="precio medio inicial")
    parser.add_argument("--vivo", action="store_true",
                        help="publicar con timestamps reales, a ritmo real")
    args = parser.parse_args()

    kafka = EventProducer()
    precio = args.precio
    ahora = datetime.now(timezone.utc)

    if args.vivo:
        t = ahora
        fin = None
    else:
        t = ahora - timedelta(minutes=args.minutos)
        fin = ahora

    enviados = 0
    try:
        while fin is None or t < fin:
            # caminata aleatoria del precio medio y spread de 0.5 a 1.5 pips
            precio += random.gauss(0, 0.00005)
            spread = random.uniform(0.00005, 0.00015)
            evento = {
                "symbol": args.simbolo,
                "bid": round(precio - spread / 2, 5),
                "ask": round(precio + spread / 2, 5),
                "timestamp": t.isoformat(),
            }
            kafka.publish(evento)
            enviados += 1

            if args.vivo:
                time.sleep(random.uniform(0.5, 1.5))
                t = datetime.now(timezone.utc)
            else:
                # 1 a 3 segundos de mercado simulado entre ticks, sin esperar
                t += timedelta(seconds=random.uniform(1, 3))
                time.sleep(0.005)
    except KeyboardInterrupt:
        pass
    finally:
        kafka.close()
        print(f"Ticks enviados: {enviados}")


if __name__ == "__main__":
    main()
