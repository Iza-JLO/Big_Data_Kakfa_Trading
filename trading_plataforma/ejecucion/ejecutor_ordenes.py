"""
Modulo 3: Gestion de riesgos y ejecucion de ordenes en Interactive Brokers (Cuenta Demo).

Consume senales desde el topico Kafka 'signals' con estructura:
    {
        "id_algoritmo": str,
        "symbol": str,
        "signal": "BUY" | "SELL",
        "precio_referencia": float,
        "timestamp": str (ISO 8601 UTC)
    }

Valida esquema, frescura, duplicados, simbolos permitidos, limites de tamano/exposicion
y verifica que la cuenta conectada en TWS sea exclusivamente DEMO (prefijo DU).
Registra todas las operaciones ejecutadas y rechazadas en consola y en logs/operaciones_audit.jsonl.
"""
import hashlib
import json
import os
import signal as sys_signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.wrapper import EWrapper
from kafka import KafkaConsumer

load_dotenv()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    return utc_now().isoformat()


class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    KAFKA_TOPIC_SIGNALS = os.getenv("KAFKA_TOPIC_SIGNALS", "signals")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ejecutor_ordenes_v1")

    # Interactive Brokers (TWS Paper Trading)
    IB_HOST = os.getenv("IB_HOST", "host.docker.internal")
    IB_PORT = int(os.getenv("IB_PORT", "7497"))
    IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "3"))
    IB_ALLOWED_ACCOUNT_PREFIX = os.getenv("IB_ALLOWED_ACCOUNT_PREFIX", "DU").strip().upper()

    # Gestion de Riesgos
    DEFAULT_ORDER_QTY = float(os.getenv("DEFAULT_ORDER_QTY", "20000"))
    MAX_ORDER_QTY = float(os.getenv("MAX_ORDER_QTY", "50000"))
    MAX_POSITION_QTY = float(os.getenv("MAX_POSITION_QTY", "50000"))
    COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "60"))
    MAX_SIGNAL_AGE_SECONDS = int(os.getenv("MAX_SIGNAL_AGE_SECONDS", "120"))
    ALLOWED_SYMBOLS = {
        s.strip().upper()
        for s in os.getenv("ALLOWED_SYMBOLS", "EURUSD").split(",")
        if s.strip()
    }
    LOG_DIR = Path(os.getenv("LOG_DIR", "/app/logs"))


class AuditLogger:
    """Registra eventos estructurados en JSONL (UTC ISO 8601) y salida estandar."""

    def __init__(self, log_dir: Path):
        log_dir.mkdir(parents=True, exist_ok=True)
        self.audit_file = log_dir / "operaciones_audit.jsonl"
        self._lock = threading.Lock()

    def log(self, estado: str, motivo: str, payload: dict | None = None, extra: dict | None = None):
        registro = {
            "timestamp_utc": utc_iso(),
            "estado": estado,
            "motivo": motivo,
            "signal": payload or {},
            "detalle": extra or {},
        }
        linea = json.dumps(registro, ensure_ascii=False)
        with self._lock:
            print(linea, flush=True)
            with self.audit_file.open("a", encoding="utf-8") as f:
                f.write(linea + "\n")


class IBExecutionApp(EWrapper, EClient):
    """Cliente de Interactive Brokers con verificacion estricta de cuenta Demo."""

    INFO_CODES = {2104, 2106, 2107, 2108, 2158}

    def __init__(self, audit: AuditLogger):
        EClient.__init__(self, self)
        self.audit = audit
        self._id_lock = threading.Lock()
        self._pos_lock = threading.Lock()
        self._next_order_id: int | None = None
        self.demo_account: str | None = None
        self.ready_event = threading.Event()
        self.positions: dict[str, float] = {}

    def nextValidId(self, orderId: int):
        with self._id_lock:
            self._next_order_id = orderId
        self._check_ready()

    def managedAccounts(self, accountsList: str):
        cuentas = [c.strip() for c in accountsList.split(",") if c.strip()]
        cuentas_demo = [
            c for c in cuentas if c.upper().startswith(Config.IB_ALLOWED_ACCOUNT_PREFIX)
        ]
        if not cuentas_demo:
            self.audit.log(
                "CRITICAL_ABORT",
                f"Ninguna cuenta coincide con prefijo demo '{Config.IB_ALLOWED_ACCOUNT_PREFIX}': {cuentas}",
            )
            return
        self.demo_account = cuentas_demo[0]
        self._check_ready()

    def _check_ready(self):
        if self._next_order_id is not None and self.demo_account is not None:
            if not self.ready_event.is_set():
                self.audit.log(
                    "IB_CONNECTED",
                    "Conexion verificada con cuenta Demo en TWS",
                    extra={
                        "account": self.demo_account,
                        "next_order_id": self._next_order_id,
                        "host": Config.IB_HOST,
                        "port": Config.IB_PORT,
                    },
                )
                self.reqPositions()
                self.ready_event.set()

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        if account != self.demo_account:
            return
        sym = (
            f"{contract.symbol}{contract.currency}"
            if contract.secType == "CASH"
            else contract.symbol
        )
        with self._pos_lock:
            self.positions[sym.upper()] = float(position)

    def get_position(self, symbol: str) -> float:
        with self._pos_lock:
            return self.positions.get(symbol.upper(), 0.0)

    def update_local_position(self, symbol: str, action: str, qty: float):
        delta = qty if action == "BUY" else -qty
        with self._pos_lock:
            self.positions[symbol.upper()] = self.positions.get(symbol.upper(), 0.0) + delta

    def orderStatus(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFillPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ):
        self.audit.log(
            "IB_ORDER_STATUS",
            f"Orden {orderId} -> {status}",
            extra={
                "order_id": orderId,
                "status": status,
                "filled": filled,
                "remaining": remaining,
                "avg_fill_price": avgFillPrice,
            },
        )

    def execDetails(self, reqId, contract, execution):
        self.audit.log(
            "IB_FILL",
            f"Ejecucion confirmada en TWS para {contract.symbol}",
            extra={
                "order_id": execution.orderId,
                "exec_id": execution.execId,
                "side": execution.side,
                "shares": execution.shares,
                "price": execution.price,
                "account": execution.acctNumber,
            },
        )

    def error(self, reqId, *args):
        if len(args) >= 3 and isinstance(args[1], int):
            errorCode, errorString = args[1], args[2]
        else:
            errorCode, errorString = args[0], args[1]

        if errorCode in self.INFO_CODES:
            return
        self.audit.log(
            "IB_MESSAGE",
            f"Codigo {errorCode}: {errorString}",
            extra={"req_id": reqId, "code": errorCode},
        )

    @staticmethod
    def build_contract(symbol: str) -> Contract:
        sym = symbol.upper().strip()
        contract = Contract()
        # Pares FOREX de 6 caracteres (ej. EURUSD) alineado con main.py
        if len(sym) == 6 and sym.isalpha():
            contract.symbol = sym[:3]
            contract.secType = "CASH"
            contract.currency = sym[3:]
            contract.exchange = "IDEALPRO"
        else:
            contract.symbol = sym
            contract.secType = "STK"
            contract.currency = "USD"
            contract.exchange = "SMART"
        return contract

    def submit_demo_order(self, symbol: str, action: str, qty: float) -> int:
        if not self.demo_account or not self.demo_account.startswith(
            Config.IB_ALLOWED_ACCOUNT_PREFIX
        ):
            raise RuntimeError("Bloqueo de seguridad: cuenta demo no verificada.")

        with self._id_lock:
            order_id = self._next_order_id
            self._next_order_id += 1

        contract = self.build_contract(symbol)
        order = Order()
        order.action = action
        order.orderType = "MKT"
        order.totalQuantity = qty
        order.account = self.demo_account
        # Evita error 10268 en versiones recientes de TWS usando ibapi 9.81
        order.eTradeOnly = False
        order.firmQuoteOnly = False

        self.placeOrder(order_id, contract, order)
        self.update_local_position(symbol, action, qty)
        return order_id


class RiskManager:
    """Motor de validacion de senales, deduplicacion y limites de riesgo."""

    REQUIRED_FIELDS = {"id_algoritmo", "symbol", "signal", "precio_referencia", "timestamp"}

    def __init__(self):
        self.seen_hashes: set[str] = set()
        self.last_execution_ts: dict[tuple[str, str], datetime] = {}

    @staticmethod
    def _parse_utc(ts_str: str) -> datetime:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _fingerprint(msg: dict) -> str:
        raw = f"{msg.get('id_algoritmo')}|{msg.get('symbol')}|{msg.get('signal')}|{msg.get('timestamp')}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def evaluate(self, msg: dict, current_position: float) -> tuple[bool, str, float]:
        """Retorna (aprobada, motivo, cantidad_a_operar)."""
        if not isinstance(msg, dict):
            return False, "Payload no es un objeto JSON valido", 0.0

        faltantes = self.REQUIRED_FIELDS - set(msg.keys())
        if faltantes:
            return False, f"Campos obligatorios faltantes: {sorted(faltantes)}", 0.0

        symbol = str(msg["symbol"]).strip().upper()
        action = str(msg["signal"]).strip().upper()

        if action not in {"BUY", "SELL"}:
            return False, f"Tipo de senal invalido: '{action}' (esperado BUY o SELL)", 0.0

        if symbol not in Config.ALLOWED_SYMBOLS:
            return False, f"Simbolo '{symbol}' fuera de ALLOWED_SYMBOLS {sorted(Config.ALLOWED_SYMBOLS)}", 0.0

        try:
            precio = float(msg["precio_referencia"])
            if precio <= 0:
                return False, f"precio_referencia debe ser > 0 (recibido: {precio})", 0.0
        except (TypeError, ValueError):
            return False, f"precio_referencia no numerico: {msg['precio_referencia']}", 0.0

        try:
            sig_dt = self._parse_utc(str(msg["timestamp"]))
        except ValueError:
            return False, f"Timestamp ISO 8601 invalido: {msg['timestamp']}", 0.0

        ahora = utc_now()
        edad = (ahora - sig_dt).total_seconds()
        if edad > Config.MAX_SIGNAL_AGE_SECONDS:
            return False, f"Senal expirada ({edad:.1f}s > {Config.MAX_SIGNAL_AGE_SECONDS}s)", 0.0

        # 1. Deduplicacion exacta
        fp = self._fingerprint(msg)
        if fp in self.seen_hashes:
            return False, f"Senal duplicada detectada (hash={fp})", 0.0

        # 2. Ventana de enfriamiento (cooldown) por (symbol, action)
        key = (symbol, action)
        prev_dt = self.last_execution_ts.get(key)
        if prev_dt is not None:
            transcurrido = (ahora - prev_dt).total_seconds()
            if transcurrido < Config.COOLDOWN_SECONDS:
                return (
                    False,
                    f"Cooldown activo para {symbol} {action} ({transcurrido:.1f}s < {Config.COOLDOWN_SECONDS}s)",
                    0.0,
                )

        # 3. Control de tamano por orden y exposicion maxima acumulada
        qty = Config.DEFAULT_ORDER_QTY
        if qty <= 0 or qty > Config.MAX_ORDER_QTY:
            return False, f"Cantidad {qty} excede MAX_ORDER_QTY ({Config.MAX_ORDER_QTY})", 0.0

        nueva_posicion = current_position + qty if action == "BUY" else current_position - qty
        if abs(nueva_posicion) > Config.MAX_POSITION_QTY:
            return (
                False,
                f"Exposicion proyectada ({nueva_posicion}) excede MAX_POSITION_QTY ({Config.MAX_POSITION_QTY})",
                0.0,
            )

        self.seen_hashes.add(fp)
        self.last_execution_ts[key] = ahora
        return True, "OK", qty


def safe_json_deserializer(raw: bytes):
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        return {"_raw_error": str(exc)}


def main():
    audit = AuditLogger(Config.LOG_DIR)

    # Bloqueo duro: nunca permitir puerto Live (7496 / 4001)
    if Config.IB_PORT in {7496, 4001}:
        audit.log(
            "CRITICAL_ABORT",
            f"Puerto {Config.IB_PORT} corresponde a cuenta REAL. Use 7497 (TWS Demo).",
        )
        sys.exit(1)

    ib_app = IBExecutionApp(audit)
    ib_app.connect(Config.IB_HOST, Config.IB_PORT, clientId=Config.IB_CLIENT_ID)

    ib_thread = threading.Thread(target=ib_app.run, name="ibapi-loop", daemon=True)
    ib_thread.start()

    if not ib_app.ready_event.wait(timeout=15):
        audit.log(
            "CRITICAL_ABORT",
            "No se recibio confirmacion de cuenta Demo desde TWS en 15s.",
        )
        ib_app.disconnect()
        sys.exit(1)

    risk = RiskManager()
    consumer = KafkaConsumer(
        Config.KAFKA_TOPIC_SIGNALS,
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=Config.KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=safe_json_deserializer,
    )

    running = True

    def _stop(signum, frame):
        nonlocal running
        running = False

    sys_signal.signal(sys_signal.SIGINT, _stop)
    sys_signal.signal(sys_signal.SIGTERM, _stop)

    audit.log(
        "MODULE_STARTED",
        f"Escuchando topico '{Config.KAFKA_TOPIC_SIGNALS}'",
        extra={
            "bootstrap_servers": Config.KAFKA_BOOTSTRAP_SERVERS,
            "group_id": Config.KAFKA_GROUP_ID,
            "allowed_symbols": sorted(Config.ALLOWED_SYMBOLS),
            "default_qty": Config.DEFAULT_ORDER_QTY,
            "max_order_qty": Config.MAX_ORDER_QTY,
            "max_position_qty": Config.MAX_POSITION_QTY,
            "cooldown_seconds": Config.COOLDOWN_SECONDS,
        },
    )

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for _tp, messages in records.items():
                for record in messages:
                    senal = record.value
                    if isinstance(senal, dict) and "_raw_error" in senal:
                        audit.log("REJECTED", f"JSON malformado: {senal['_raw_error']}")
                        continue

                    symbol = str(senal.get("symbol", "")).upper() if isinstance(senal, dict) else ""
                    pos_actual = ib_app.get_position(symbol)
                    aprobada, motivo, qty = risk.evaluate(senal, pos_actual)

                    if not aprobada:
                        audit.log("REJECTED", motivo, payload=senal)
                        continue

                    try:
                        order_id = ib_app.submit_demo_order(
                            symbol=symbol,
                            action=senal["signal"].upper(),
                            qty=qty,
                        )
                        audit.log(
                            "EXECUTED",
                            "Orden enviada a TWS Paper Trading",
                            payload=senal,
                            extra={
                                "order_id": order_id,
                                "account": ib_app.demo_account,
                                "qty": qty,
                                "posicion_proyectada": ib_app.get_position(symbol),
                            },
                        )
                    except Exception as exc:
                        audit.log(
                            "REJECTED",
                            f"Error al enviar orden a TWS: {exc}",
                            payload=senal,
                        )
    finally:
        audit.log("MODULE_STOPPED", "Cerrando consumidor Kafka y conexion TWS")
        consumer.close()
        ib_app.disconnect()


if __name__ == "__main__":
    main()