"""
Algoritmo de cruce de medias moviles (SMA rapida 5 / SMA lenta 20).

Consume velas del topico "velas" y publica senales BUY/SELL en "signals":
    {id_algoritmo, symbol, signal, precio_referencia, timestamp}

Regla:
    BUY  cuando la media rapida cruza por ENCIMA de la lenta.
    SELL cuando la media rapida cruza por DEBAJO de la lenta.
Solo se emite senal en el momento del cruce, no en cada vela.

La logica esta en la clase CruceMedias, que NO depende de Kafka. Asi el mismo
codigo se usa en vivo (este archivo) y en backtesting (backtesting.py).

Uso (con Kafka corriendo y velas en el topico):
    python algoritmo_trading.py
    python algoritmo_trading.py --solo-nuevas     # recomendado en vivo
"""
import argparse
import json
import uuid
from collections import deque
from datetime import datetime, timezone

BOOTSTRAP = "localhost:9092"
TOPIC_VELAS = "velas"
TOPIC_SIGNALS = "signals"
ID_ALGORITMO = "cruce_sma_5_20"


class CruceMedias:
    def __init__(self, rapida=5, lenta=20, id_algoritmo=ID_ALGORITMO):
        if rapida >= lenta:
            raise ValueError("La media rapida debe ser menor que la lenta")
        self.rapida = rapida
        self.lenta = lenta
        self.id_algoritmo = id_algoritmo
        self.cierres = {}         # symbol -> ultimos cierres (maximo "lenta")
        self.ultimo_inicio = {}   # symbol -> inicio de la ultima vela procesada
        self.diff_previa = {}     # symbol -> (SMA rapida - SMA lenta) anterior

    def on_vela(self, vela):
        """Procesa una vela. Devuelve un dict de senal o None."""
        symbol = vela["symbol"]
        inicio = vela["inicio"]

        # Spark puede republicar velas (por ejemplo si se borran checkpoints):
        # se ignoran las repetidas y las atrasadas.
        ultimo = self.ultimo_inicio.get(symbol)
        if ultimo is not None and inicio <= ultimo:
            return None
        self.ultimo_inicio[symbol] = inicio

        cierres = self.cierres.setdefault(symbol, deque(maxlen=self.lenta))
        cierres.append(float(vela["close"]))
        if len(cierres) < self.lenta:
            return None  # todavia no hay suficientes velas

        sma_rapida = sum(list(cierres)[-self.rapida:]) / self.rapida
        sma_lenta = sum(cierres) / self.lenta
        diff = sma_rapida - sma_lenta

        previa = self.diff_previa.get(symbol)
        self.diff_previa[symbol] = diff
        if previa is None:
            return None

        if previa <= 0 < diff:
            senal = "BUY"
        elif previa >= 0 > diff:
            senal = "SELL"
        else:
            return None

        return {
            "id_algoritmo": self.id_algoritmo,
            "symbol": symbol,
            "signal": senal,
            "precio_referencia": float(vela["close"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def _a_datetime(texto):
    dt = datetime.fromisoformat(texto.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rapida", type=int, default=5)
    parser.add_argument("--lenta", type=int, default=20)
    parser.add_argument(
        "--solo-nuevas", action="store_true",
        help="no publicar senales de velas anteriores al arranque del programa")
    args = parser.parse_args()

    from kafka import KafkaConsumer, KafkaProducer  # import aqui: el backtest no lo necesita

    arranque = datetime.now(timezone.utc)
    estrategia = CruceMedias(args.rapida, args.lenta)

    # Se lee desde el principio para reconstruir las medias al arrancar.
    # group_id unico + sin commits: cada arranque relee todo el historico.
    consumer = KafkaConsumer(
        TOPIC_VELAS,
        bootstrap_servers=BOOTSTRAP,
        group_id=f"{ID_ALGORITMO}-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"{ID_ALGORITMO} escuchando '{TOPIC_VELAS}' (Ctrl+C para salir)...")
    try:
        for mensaje in consumer:
            vela = mensaje.value
            senal = estrategia.on_vela(vela)
            if senal is None:
                continue
            if args.solo_nuevas and _a_datetime(vela["inicio"]) < arranque:
                continue  # senal historica: se usa para calentar, no se publica
            producer.send(TOPIC_SIGNALS, key=senal["symbol"], value=senal)
            producer.flush()
            print("SENAL:", json.dumps(senal))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
