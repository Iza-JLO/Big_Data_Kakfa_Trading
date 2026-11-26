"""
Este main gestiona las solicitudes hacia el IBKR, recibe las respuestas del mismo
y define los instrumentos (FOREX) que vamos a consultar. Va a solicitar las 
cotizaciones de la misma.
"""
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from datetime import datetime, timezone
import json
from kafka_productor import EventProducer


class IBProducer(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.kafka = EventProducer()
        self.bid = None
        self.ask = None

    def nextValidId(self, orderId):
        print("Conexión Establecida xd")

        contract = Contract()
        contract.symbol = "EUR"
        contract.secType = "CASH"
        contract.currency = "USD"
        contract.exchange = "IDEALPRO"

        self.reqMktData(1, contract, "", False, False, [])
        print("Solicitud de cotizaciones:")

    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 1:
            self.bid = price
        elif tickType == 2:
            self.ask = price
        if self.bid is not None and self.ask is not None:

            event = {
                "symbol": "EURUSD",
                "bid": self.bid,
                "ask": self.ask,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            print(json.dumps(event))
            self.kafka.publish(event)

    def error(self, reqId, *args):
        if len(args) >= 3 and isinstance(args[1], int):
            errorCode, errorString = args[1], args[2]
        else:
            errorCode, errorString = args[0], args[1]
        print(f"Mensaje de IB {errorCode}: {errorString}")


if __name__ == "__main__":
    app = IBProducer()
    app.connect("127.0.0.1", 7497, clientId=1)
    print("Conectando con Trader Work Station")

    try:
        app.run()
    except KeyboardInterrupt:
        print("Desconentando app")
    finally:
        app.disconnect()
        app.kafka.close()