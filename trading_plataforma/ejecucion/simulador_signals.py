"""
Simulador de senales para pruebas independientes del Modulo 3 (ejecutor_ordenes.py).

Publica eventos en el topico Kafka 'signals' respetando el contrato del Integrante 2:
    {id_algoritmo, symbol, signal, precio_referencia, timestamp}

Ejemplos de uso desde Docker:
    python simulador_signals.py --modo suite
    python simulador_signals.py --modo manual --symbol EURUSD --signal BUY --precio 1.0850
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_SIGNALS = os.getenv("KAFKA_TOPIC_SIGNALS", "signals")


def crear_productor() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def enviar(producer: KafkaProducer, evento: dict, descripcion: str):
    key = str(evento.get("symbol", "UNKNOWN"))
    producer.send(TOPIC_SIGNALS, key=key, value=evento)
    producer.flush()
    print(f"[ENVIADO - {descripcion}] {json.dumps(evento)}")


def ejecutar_suite(producer: KafkaProducer):
    ahora = datetime.now(timezone.utc)

    # 1. Senal valida BUY -> Debe EJECUTARSE en TWS Paper
    senal_buy = {
        "id_algoritmo": "cruce_sma_5_20_test",
        "symbol": "EURUSD",
        "signal": "BUY",
        "precio_referencia": 1.0850,
        "timestamp": ahora.isoformat(),
    }
    enviar(producer, senal_buy, "1. BUY valido (esperado: EXECUTED)")
    time.sleep(2)

    # 2. Duplicado exacto -> Debe RECHAZARSE
    enviar(producer, senal_buy, "2. Duplicado exacto (esperado: REJECTED duplicado)")
    time.sleep(1)

    # 3. Misma direccion dentro de cooldown -> Debe RECHAZARSE
    senal_cooldown = {
        "id_algoritmo": "cruce_sma_5_20_test",
        "symbol": "EURUSD",
        "signal": "BUY",
        "precio_referencia": 1.0855,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    enviar(producer, senal_cooldown, "3. BUY en cooldown (esperado: REJECTED cooldown)")
    time.sleep(1)

    # 4. Simbolo no autorizado -> Debe RECHAZARSE
    senal_simbolo = {
        "id_algoritmo": "cruce_sma_5_20_test",
        "symbol": "BTCUSD",
        "signal": "BUY",
        "precio_referencia": 92000.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    enviar(producer, senal_simbolo, "4. Simbolo no permitido (esperado: REJECTED simbolo)")
    time.sleep(1)

    # 5. Senal expirada (hace 10 minutos) -> Debe RECHAZARSE
    senal_vieja = {
        "id_algoritmo": "cruce_sma_5_20_test",
        "symbol": "EURUSD",
        "signal": "SELL",
        "precio_referencia": 1.0840,
        "timestamp": (ahora - timedelta(minutes=10)).isoformat(),
    }
    enviar(producer, senal_vieja, "5. Senal expirada (esperado: REJECTED expirada)")
    time.sleep(2)

    # 6. Senal valida SELL -> Debe EJECUTARSE y cerrar la posicion abierta en paso 1
    senal_sell = {
        "id_algoritmo": "cruce_sma_5_20_test",
        "symbol": "EURUSD",
        "signal": "SELL",
        "precio_referencia": 1.0845,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    enviar(producer, senal_sell, "6. SELL valido cierre (esperado: EXECUTED)")


def main():
    parser = argparse.ArgumentParser(description="Simulador de senales para Modulo 3")
    parser.add_argument("--modo", choices=["suite", "manual"], default="suite")
    parser.add_argument("--symbol", default="EURUSD")
    parser.add_argument("--signal", choices=["BUY", "SELL"], default="BUY")
    parser.add_argument("--precio", type=float, default=1.0850)
    args = parser.parse_args()

    producer = crear_productor()
    try:
        if args.modo == "suite":
            ejecutar_suite(producer)
        else:
            evento = {
                "id_algoritmo": "manual_test",
                "symbol": args.symbol.upper(),
                "signal": args.signal.upper(),
                "precio_referencia": args.precio,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            enviar(producer, evento, "MANUAL")
    finally:
        producer.close()


if __name__ == "__main__":
    main()