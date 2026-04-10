"""Anten — AI trading cockpit (Telegram bot + desktop UI).

Runs paper-trading by default; Telegram is optional via `ANTEN_TELEGRAM_TOKEN`.
Local state is SQLite under your home directory.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import json
import math
import os
import queue
import random
import re
import secrets
import sqlite3
import sys
import threading
import time
import traceback
import typing as t
import uuid

try:
    import tkinter as tk
    from tkinter import ttk, filedialog
except Exception as _e:  # pragma: no cover
    tk = None
    ttk = None
    filedialog = None
# ---- UTIL ----
APP_NAME = "Anten"
APP_VERSION = "0.8.7"
class AError(Exception):
    pass


class ConfigError(AError):
    pass


class StorageError(AError):
    pass


class RiskError(AError):
    pass


class BrokerError(AError):
    pass


class SignalError(AError):
    pass


def now_utc() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.timezone.utc)


def utc_ts() -> int:
    return int(time.time())


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def safe_float(x: t.Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return float(int(x))
        return float(x)
    except Exception:
        return default


def safe_int(x: t.Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return int(x)
        return int(float(x))
    except Exception:
        return default


def short_id(prefix: str = "") -> str:
    s = uuid.uuid4().hex[:10]
    return f"{prefix}{s}" if prefix else s


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def jitter_sleep(base: float, spread: float = 0.35) -> None:
    r = random.random() * spread
    time.sleep(max(0.0, base - spread / 2 + r))


def human_dt(ts: int) -> str:
    try:
        return _dt.datetime.fromtimestamp(ts, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


def fmt_money(x: float, sym: str = "$") -> str:
    try:
        return f"{sym}{x:,.2f}"
    except Exception:
        return f"{sym}{x}"


def fmt_pct(x: float) -> str:
    try:
        return f"{x*100:.2f}%"
    except Exception:
        return f"{x}%"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def app_home() -> str:
    base = os.path.expanduser("~")
    p = os.path.join(base, f".{APP_NAME.lower()}")
    ensure_dir(p)
    return p


def env(name: str, default: str | None = None) -> str | None:
    v = os.environ.get(name)
    if v is None or v.strip() == "":
        return default
    return v.strip()


def log_level() -> str:
    return (env("ANTEN_LOG_LEVEL", "INFO") or "INFO").upper()


def is_debug() -> bool:
    return log_level() in {"DEBUG", "TRACE"}


class Logger:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._level = log_level()
        self._levels = {"TRACE": 5, "DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}

    def _ok(self, lvl: str) -> bool:
        return self._levels.get(lvl, 20) >= self._levels.get(self._level, 20)

    def _emit(self, lvl: str, msg: str) -> None:
        with self._lock:
            ts = now_utc().strftime("%H:%M:%S")
            out = f"[{ts}] {lvl:<5} {msg}"
            print(out, file=sys.stderr if lvl in {"WARN", "ERROR"} else sys.stdout, flush=True)

    def trace(self, msg: str) -> None:
        if self._ok("TRACE"):
            self._emit("TRACE", msg)

    def debug(self, msg: str) -> None:
        if self._ok("DEBUG"):
            self._emit("DEBUG", msg)

    def info(self, msg: str) -> None:
        if self._ok("INFO"):
            self._emit("INFO", msg)

    def warn(self, msg: str) -> None:
        if self._ok("WARN"):
            self._emit("WARN", msg)

    def error(self, msg: str) -> None:
        if self._ok("ERROR"):
            self._emit("ERROR", msg)


LOG = Logger()


def exc_to_str(e: BaseException) -> str:
    return "".join(traceback.format_exception(type(e), e, e.__traceback__)).strip()


# ---- CONFIG ----


@dataclasses.dataclass(frozen=True)
class AppIdentity:
    run_id: str
    instance_salt: str


@dataclasses.dataclass
class AppConfig:
    # UI
    theme: str = "dark"
    ui_tick_ms: int = 150
    show_advanced: bool = True

    # Signal filters
    allowed_markets: list[str] = dataclasses.field(default_factory=list)
    min_confidence: float = 0.55
    max_signals_per_min: int = 60

    # Paper broker defaults
    account_currency: str = "USD"
    starting_balance: float = 25_000.0
    maker_fee_bps: float = 2.0
    taker_fee_bps: float = 7.0
    max_leverage: float = 5.0
    risk_per_trade: float = 0.008
    max_open_positions: int = 7

    # Risk controls
    max_daily_loss_pct: float = 0.06
    max_drawdown_pct: float = 0.18
    cooldown_sec: int = 8
    kill_switch: bool = False

    # Telegram
    telegram_enabled: bool = True
    telegram_chat_allowlist: list[int] = dataclasses.field(default_factory=list)
    telegram_admins: list[int] = dataclasses.field(default_factory=list)

    # Storage paths
    db_path: str = ""
    export_dir: str = ""

    def normalize(self) -> None:
        if self.ui_tick_ms < 50:
            self.ui_tick_ms = 50
        if self.ui_tick_ms > 2000:
            self.ui_tick_ms = 2000
        self.min_confidence = clamp(self.min_confidence, 0.0, 1.0)
        self.max_signals_per_min = max(1, min(10_000, int(self.max_signals_per_min)))
        self.starting_balance = max(0.0, float(self.starting_balance))
        self.max_leverage = clamp(float(self.max_leverage), 1.0, 50.0)
        self.risk_per_trade = clamp(float(self.risk_per_trade), 0.0001, 0.25)
        self.max_open_positions = max(1, min(200, int(self.max_open_positions)))
        self.max_daily_loss_pct = clamp(float(self.max_daily_loss_pct), 0.0, 0.99)
        self.max_drawdown_pct = clamp(float(self.max_drawdown_pct), 0.0, 0.99)
        self.cooldown_sec = max(0, min(3600, int(self.cooldown_sec)))

        if not self.db_path:
            self.db_path = env("ANTEN_DB_PATH") or os.path.join(app_home(), "anten.sqlite3")
        if not self.export_dir:
            self.export_dir = os.path.join(app_home(), "exports")
        ensure_dir(os.path.dirname(self.db_path))
        ensure_dir(self.export_dir)


class ConfigStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()

    def load(self) -> AppConfig:
        with self._lock:
            if not os.path.exists(self.path):
                cfg = AppConfig()
                cfg.normalize()
                return cfg
            try:
                raw = json.loads(open(self.path, "r", encoding="utf-8").read())
                cfg = AppConfig(**raw)
                cfg.normalize()
                return cfg
            except Exception as e:
                raise ConfigError(f"Could not load config: {e}") from e

    def save(self, cfg: AppConfig) -> None:
        with self._lock:
            try:
                cfg.normalize()
                tmp = self.path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(dataclasses.asdict(cfg), f, indent=2, sort_keys=True)
                os.replace(tmp, self.path)
            except Exception as e:
                raise ConfigError(f"Could not save config: {e}") from e


# ---- STORAGE (SQL) ----


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS kv (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
  id TEXT PRIMARY KEY,
  t INTEGER NOT NULL,
  market TEXT NOT NULL,
  strat TEXT NOT NULL,
  direction INTEGER NOT NULL,
  confidence REAL NOT NULL,
  notional_hint REAL NOT NULL,
  salt TEXT NOT NULL,
  meta_hash TEXT NOT NULL,
  raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signals_t ON signals(t);
CREATE INDEX IF NOT EXISTS idx_signals_market ON signals(market, t);

CREATE TABLE IF NOT EXISTS trades (
  id TEXT PRIMARY KEY,
  t_open INTEGER NOT NULL,
  t_close INTEGER,
  market TEXT NOT NULL,
  side INTEGER NOT NULL,
  qty REAL NOT NULL,
  entry REAL NOT NULL,
  exit REAL,
  fee REAL NOT NULL,
  pnl REAL NOT NULL,
  status TEXT NOT NULL,
  reason TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_open ON trades(t_open);
CREATE INDEX IF NOT EXISTS idx_trades_close ON trades(t_close);
"""


class SqliteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._setup()

    def _setup(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    def kv_get(self, k: str, default: str | None = None) -> str | None:
        with self._lock:
            cur = self._conn.execute("SELECT v FROM kv WHERE k=?", (k,))
            row = cur.fetchone()
            return row["v"] if row else default

    def kv_set(self, k: str, v: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (k, v),
            )
            self._conn.commit()

    def add_signal(self, s: "Signal") -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO signals
                (id,t,market,strat,direction,confidence,notional_hint,salt,meta_hash,raw_json)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    s.id,
                    s.t,
                    s.market,
                    s.strat,
                    s.direction,
                    float(s.confidence),
                    float(s.notional_hint),
                    s.salt,
                    s.meta_hash,
                    json.dumps(s.raw, separators=(",", ":"), sort_keys=True),
                ),
            )
            self._conn.commit()

    def list_signals(self, limit: int = 200, market: str | None = None) -> list[dict]:
        with self._lock:
            if market:
                cur = self._conn.execute(
                    "SELECT * FROM signals WHERE market=? ORDER BY t DESC LIMIT ?",
                    (market, int(limit)),
                )
            else:
                cur = self._conn.execute(
                    "SELECT * FROM signals ORDER BY t DESC LIMIT ?",
                    (int(limit),),
                )
            return [dict(r) for r in cur.fetchall()]

    def add_trade(self, tr: "Trade") -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO trades
                (id,t_open,t_close,market,side,qty,entry,exit,fee,pnl,status,reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    tr.id,
                    tr.t_open,
                    tr.t_close,
                    tr.market,
                    tr.side,
                    float(tr.qty),
                    float(tr.entry),
                    None if tr.exit is None else float(tr.exit),
                    float(tr.fee),
                    float(tr.pnl),
                    tr.status,
                    tr.reason,
                ),
            )
            self._conn.commit()

    def list_trades(self, limit: int = 300) -> list[dict]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM trades ORDER BY t_open DESC LIMIT ?", (int(limit),))
            return [dict(r) for r in cur.fetchall()]


# ---- EVENT BUS ----


@dataclasses.dataclass(frozen=True)
class Event:
    topic: str
    t: int
    payload: dict


class EventBus:
    def __init__(self) -> None:
        self._subs: dict[str, list[t.Callable[[Event], None]]] = {}
        self._q: "queue.Queue[Event]" = queue.Queue()
        self._stop = threading.Event()
        self._thr = threading.Thread(target=self._run, name="eventbus", daemon=True)

    def start(self) -> None:
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        self._q.put(Event("bus/stop", utc_ts(), {}))
        self._thr.join(timeout=2.0)

    def subscribe(self, topic: str, fn: t.Callable[[Event], None]) -> None:
        self._subs.setdefault(topic, []).append(fn)

    def publish(self, topic: str, payload: dict) -> None:
        self._q.put(Event(topic, utc_ts(), payload))

    def _deliver(self, ev: Event) -> None:
        # exact
        for fn in self._subs.get(ev.topic, []):
            try:
                fn(ev)
            except Exception as e:
                LOG.warn(f"subscriber error topic={ev.topic}: {e}")
        # prefix wildcards (topic/*)
        for tpc, fns in list(self._subs.items()):
            if tpc.endswith("/*"):
                prefix = tpc[:-2]
                if ev.topic.startswith(prefix):
                    for fn in fns:
                        try:
                            fn(ev)
                        except Exception as e:
                            LOG.warn(f"subscriber error topic={tpc}: {e}")

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                ev = self._q.get(timeout=0.2)
            except queue.Empty:
                continue
            if ev.topic == "bus/stop":
                break
            self._deliver(ev)


# ---- SIGNALS ----


@dataclasses.dataclass
class Signal:
    id: str
    t: int
    market: str
    strat: str
    direction: int  # -1 short, 0 flat, +1 long
    confidence: float
    notional_hint: float
    salt: str
    meta_hash: str
    raw: dict

    def score(self) -> float:
        # Simple ranking: confidence * direction magnitude (flat becomes 0)
        return abs(self.direction) * self.confidence


class SignalCodec:
    MARKET_RE = re.compile(r"^[A-Z0-9]{2,12}[:/][A-Z0-9]{2,12}(/[\w\-]{1,10})?$")

    def __init__(self, identity: AppIdentity) -> None:
        self.identity = identity

    def _canon_market(self, m: str) -> str:
        m = (m or "").strip().upper()
        m = m.replace("-", "/").replace(" ", "")
        if ":" not in m and "/" in m:
            # allow "BTC/USDT" -> "BTC:USDT"
            m = m.replace("/", ":")
        return m

    def parse(self, raw: dict) -> Signal:
        if not isinstance(raw, dict):
            raise SignalError("signal must be an object")

        market = self._canon_market(str(raw.get("market", "") or ""))
        if not market or len(market) > 40:
            raise SignalError("bad market")

        strat = str(raw.get("strat", "") or raw.get("strategy", "") or "").strip()
        if not strat:
            strat = "strat/" + sha256_hex(json.dumps(raw, sort_keys=True))[:12]
        if len(strat) > 80:
            strat = strat[:80]

        direction = safe_int(raw.get("direction", raw.get("side", 0)), 0)
        if direction not in (-1, 0, 1):
            # allow textual
            s = str(raw.get("direction", "")).lower()
            if s in {"long", "buy", "1", "+1"}:
                direction = 1
            elif s in {"short", "sell", "-1"}:
                direction = -1
            else:
                direction = 0

        confidence = safe_float(raw.get("confidence", raw.get("conf", 0.5)), 0.5)
        confidence = clamp(confidence, 0.0, 1.0)

        notional_hint = safe_float(raw.get("notional_hint", raw.get("notional", 0.0)), 0.0)
        notional_hint = max(0.0, notional_hint)

        meta_hash = str(raw.get("meta_hash", raw.get("metaHash", "")) or "").strip()
        if not meta_hash:
            # derive a stable hash from raw payload + instance salt (avoids collisions in UI)
            meta_hash = sha256_hex(self.identity.instance_salt + json.dumps(raw, sort_keys=True))[:64]

        salt = str(raw.get("salt", "") or "").strip()
        if not salt:
            salt = secrets.token_hex(16)

        t0 = safe_int(raw.get("t", raw.get("time", utc_ts())), utc_ts())
        if t0 < 0:
            t0 = utc_ts()

        sid = str(raw.get("id", "") or "").strip()
        if not sid:
            sid = "sig_" + sha256_hex(f"{t0}|{market}|{strat}|{direction}|{confidence}|{salt}|{meta_hash}")[:20]

        return Signal(
            id=sid,
            t=t0,
            market=market,
            strat=strat,
            direction=direction,
            confidence=confidence,
            notional_hint=notional_hint,
            salt=salt,
            meta_hash=meta_hash,
            raw=raw,
        )


class SignalLimiter:
    def __init__(self, max_per_min: int) -> None:
        self.max_per_min = max(1, int(max_per_min))
        self._lock = threading.Lock()
        self._bucket: list[int] = []

    def allow(self, t_now: int | None = None) -> bool:
        t_now = utc_ts() if t_now is None else int(t_now)
        with self._lock:
            cutoff = t_now - 60
            self._bucket = [t for t in self._bucket if t >= cutoff]
            if len(self._bucket) >= self.max_per_min:
                return False
            self._bucket.append(t_now)
            return True


# ---- PAPER BROKER ----


@dataclasses.dataclass
class Position:
    market: str
    side: int  # +1 long, -1 short
    qty: float
    entry: float
    t_open: int
    max_favorable: float = 0.0
    max_adverse: float = 0.0

    def mark(self, px: float) -> dict:
        if self.side == 1:
            pnl = (px - self.entry) * self.qty
        else:
            pnl = (self.entry - px) * self.qty
        return {"pnl": pnl, "roi": pnl / max(1e-9, abs(self.entry * self.qty))}


@dataclasses.dataclass
class Trade:
    id: str
    t_open: int
    t_close: int | None
    market: str
    side: int
    qty: float
    entry: float
    exit: float | None
    fee: float
    pnl: float
    status: str  # OPEN/CLOSED/REJECTED
    reason: str


class PriceOracle:
    """Pseudo-oracle for paper trading (seeded random walk + snapshots)."""

    def __init__(self, identity: AppIdentity) -> None:
        self.identity = identity
        self._lock = threading.Lock()
        self._state: dict[str, dict] = {}

    def _seed(self, market: str) -> int:
        return int(hashlib.sha256((self.identity.instance_salt + market).encode()).hexdigest()[:8], 16)

    def get(self, market: str) -> float:
        with self._lock:
            st = self._state.get(market)
            if st is None:
                rnd = random.Random(self._seed(market))
                base = 100.0 + rnd.random() * 900.0
                st = {
                    "px": base,
                    "t": utc_ts(),
                    "rnd": rnd,
                    "vol": 0.007 + rnd.random() * 0.02,
                }
                self._state[market] = st
            t_now = utc_ts()
            dt = max(1, t_now - int(st["t"]))
            st["t"] = t_now
            px = float(st["px"])
            vol = float(st["vol"])
            rnd = st["rnd"]
            for _ in range(min(60, dt)):
                step = rnd.gauss(0, 1) * vol
                px *= math.exp(step)
            st["px"] = px
            return px

    def set_snapshot(self, market: str, px: float) -> None:
        with self._lock:
            st = self._state.get(market)
            if st is None:
                st = {"px": float(px), "t": utc_ts(), "rnd": random.Random(self._seed(market)), "vol": 0.012}
                self._state[market] = st
            st["px"] = float(px)
            st["t"] = utc_ts()


class RiskEngine:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self._lock = threading.Lock()
        self._day0 = utc_ts()
        self._day_start_equity = cfg.starting_balance
        self._equity_peak = cfg.starting_balance
        self._cooldowns: dict[str, int] = {}

    def reset_day(self, equity: float) -> None:
        with self._lock:
            self._day0 = utc_ts()
            self._day_start_equity = equity
            self._cooldowns = {}

    def update_equity(self, equity: float) -> None:
        with self._lock:
            self._equity_peak = max(self._equity_peak, equity)

    def check(self, market: str, equity: float) -> None:
        if self.cfg.kill_switch:
            raise RiskError("kill switch is on")

        t_now = utc_ts()
        if t_now - self._day0 > 24 * 3600:
            self.reset_day(equity)

        daily_loss = (equity - self._day_start_equity) / max(1e-9, self._day_start_equity)
        if daily_loss < -abs(self.cfg.max_daily_loss_pct):
            raise RiskError(f"daily loss limit hit ({fmt_pct(daily_loss)})")

        drawdown = (equity - self._equity_peak) / max(1e-9, self._equity_peak)
        if drawdown < -abs(self.cfg.max_drawdown_pct):
            raise RiskError(f"drawdown limit hit ({fmt_pct(drawdown)})")

        cd = self._cooldowns.get(market, 0)
        if self.cfg.cooldown_sec and t_now < cd:
            raise RiskError(f"cooldown active for {market} ({cd - t_now}s)")

    def cool(self, market: str) -> None:
        if self.cfg.cooldown_sec <= 0:
            return
        self._cooldowns[market] = utc_ts() + int(self.cfg.cooldown_sec)


class PaperBroker:
    def __init__(self, cfg: AppConfig, oracle: PriceOracle, store: SqliteStore, risk: RiskEngine) -> None:
        self.cfg = cfg
        self.oracle = oracle
        self.store = store
        self.risk = risk
        self._lock = threading.RLock()
        self.cash = float(cfg.starting_balance)
        self.positions: dict[str, Position] = {}
        self._fees_paid = 0.0
        self._trade_count = 0

    def equity(self) -> float:
        with self._lock:
            eq = self.cash
            for p in self.positions.values():
                px = self.oracle.get(p.market)
                eq += p.mark(px)["pnl"]
            return float(eq)

    def stats(self) -> dict:
        with self._lock:
            return {
                "cash": self.cash,
                "equity": self.equity(),
                "open_positions": len(self.positions),
                "fees_paid": self._fees_paid,
                "trade_count": self._trade_count,
            }

    def _fee(self, notional: float, taker: bool = True) -> float:
        bps = self.cfg.taker_fee_bps if taker else self.cfg.maker_fee_bps
        return abs(notional) * (bps / 10_000.0)

    def _qty_from_risk(self, px: float, equity: float) -> float:
        # risk per trade is applied to notional; simplistic but controllable.
        risk_budget = equity * float(self.cfg.risk_per_trade)
        if risk_budget <= 0:
            return 0.0
        notional = risk_budget * float(self.cfg.max_leverage)
        qty = notional / max(1e-9, px)
        return max(0.0, qty)

    def open_from_signal(self, s: Signal, reason: str = "signal") -> Trade:
        with self._lock:
            px = self.oracle.get(s.market)
            eq = self.equity()
            self.risk.check(s.market, eq)

            if len(self.positions) >= int(self.cfg.max_open_positions):
                raise BrokerError("max open positions reached")

            if s.direction == 0:
                raise BrokerError("flat signal not tradable")

            if s.market in self.positions:
                # already in position; treat as no-op
                raise BrokerError("position already open")

            qty = self._qty_from_risk(px, eq)
            if qty <= 0:
                raise BrokerError("qty computed as zero")

            # apply confidence as fraction of size
            qty *= clamp(float(s.confidence), 0.05, 1.0)

            notional = qty * px
            fee = self._fee(notional, taker=True)
            if self.cash - fee < 0:
                raise BrokerError("insufficient cash for fees")

            self.cash -= fee
            self._fees_paid += fee
            self._trade_count += 1

            pos = Position(market=s.market, side=int(s.direction), qty=float(qty), entry=float(px), t_open=utc_ts())
            self.positions[s.market] = pos

            tr = Trade(
                id="tr_" + short_id(),
                t_open=pos.t_open,
                t_close=None,
                market=s.market,
                side=pos.side,
                qty=pos.qty,
                entry=pos.entry,
                exit=None,
                fee=fee,
                pnl=0.0,
                status="OPEN",
                reason=reason,
            )
            self.store.add_trade(tr)
            self.risk.cool(s.market)
            return tr

    def close(self, market: str, reason: str = "close") -> Trade:
        with self._lock:
            if market not in self.positions:
                raise BrokerError("no such position")
            pos = self.positions.pop(market)
            px = self.oracle.get(market)
            mark = pos.mark(px)
            pnl = float(mark["pnl"])
            notional = pos.qty * px
            fee = self._fee(notional, taker=True)
            self.cash += pnl
            self.cash -= fee
            self._fees_paid += fee
            self._trade_count += 1

            tr = Trade(
                id="tr_" + short_id(),
                t_open=pos.t_open,
                t_close=utc_ts(),
                market=pos.market,
                side=pos.side,
                qty=pos.qty,
                entry=pos.entry,
                exit=float(px),
                fee=fee,
                pnl=pnl - fee,
                status="CLOSED",
                reason=reason,
            )
            self.store.add_trade(tr)
            self.risk.cool(market)
            return tr

    def close_all(self, reason: str = "close_all") -> list[Trade]:
        out: list[Trade] = []
        for m in list(self.positions.keys()):
            try:
                out.append(self.close(m, reason=reason))
            except Exception as e:
                LOG.warn(f"close_all failed for {m}: {e}")
        return out


# ---- STRATEGY ROUTER ----


class StrategyRouter:
    """Conservative router: filters signals and opens paper trades (no auto-flips)."""

    def __init__(self, cfg: AppConfig, limiter: SignalLimiter, broker: PaperBroker, store: SqliteStore) -> None:
        self.cfg = cfg
        self.limiter = limiter
        self.broker = broker
        self.store = store
        self._lock = threading.Lock()

    def accept(self, s: Signal) -> bool:
        if not self.limiter.allow(s.t):
            return False
        if s.confidence < float(self.cfg.min_confidence):
            return False
        if self.cfg.allowed_markets and s.market not in set(m.upper() for m in self.cfg.allowed_markets):
            return False
        return True

    def on_signal(self, s: Signal) -> dict:
        with self._lock:
            if not self.accept(s):
                return {"status": "ignored", "reason": "filtered"}

            self.store.add_signal(s)

            if self.cfg.kill_switch:
                return {"status": "ignored", "reason": "kill_switch"}

            # Only open trades when we are flat on that market.
            if s.market in self.broker.positions:
                return {"status": "ignored", "reason": "already_in_position"}

            try:
                tr = self.broker.open_from_signal(s, reason=f"{s.strat}:{s.id}")
                return {"status": "opened", "trade_id": tr.id, "market": tr.market, "side": tr.side}
            except Exception as e:
                return {"status": "rejected", "reason": str(e)}


# ---- TELEGRAM BOT (OPTIONAL) ----


class TelegramBotRunner:
    """Optional Telegram bot (enabled by `ANTEN_TELEGRAM_TOKEN`)."""

    def __init__(self, cfg: AppConfig, bus: EventBus) -> None:
        self.cfg = cfg
        self.bus = bus
        self.token = env("ANTEN_TELEGRAM_TOKEN")
        self._enabled = bool(self.token and cfg.telegram_enabled)
        self._thr: threading.Thread | None = None
        self._stop = threading.Event()
        self._app = None
        self._last_push: dict[int, int] = {}

    def enabled(self) -> bool:
        return self._enabled

    def start(self) -> None:
        if not self._enabled:
            LOG.info("Telegram: disabled (no token or disabled in config)")
            return
        self._thr = threading.Thread(target=self._run, name="telegram", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._app is not None:
                # best-effort stop (async lib)
                pass
        except Exception:
            pass

    def push(self, chat_id: int, text: str) -> None:
        # Only used if the PTB app is running; otherwise ignored.
        try:
            if self._app is None:
                return
            # rate limit pushes per chat
            t_now = utc_ts()
            last = self._last_push.get(chat_id, 0)
            if t_now - last < 1:
                return
            self._last_push[chat_id] = t_now
            self._app.bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            LOG.debug(f"Telegram push failed: {e}")

    def _run(self) -> None:
        try:
            from telegram import Update
            from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
        except Exception as e:
            LOG.warn(f"Telegram: missing dependency. Install python-telegram-bot. ({e})")
            self._enabled = False
            return

        token = self.token
        if not token:
            self._enabled = False
            return

        async def _start(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._chat_allowed(chat_id):
                await update.message.reply_text("Anten: access denied.")
                return
            await update.message.reply_text(
                f"Anten online.\nVersion: {APP_VERSION}\n"
                f"Commands: /status /kill /unkill /positions /closeall /help"
            )

        async def _help(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._chat_allowed(chat_id):
                await update.message.reply_text("Anten: access denied.")
                return
            await update.message.reply_text(
                "Anten commands:\n"
                "/status — equity snapshot\n"
                "/positions — open positions\n"
                "/closeall — close all paper positions\n"
                "/kill — enable kill switch\n"
                "/unkill — disable kill switch\n"
                "You can also paste a JSON signal object."
            )

        async def _status(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._chat_allowed(chat_id):
                await update.message.reply_text("Anten: access denied.")
                return
            self.bus.publish("tg/status", {"chat_id": chat_id})
            await update.message.reply_text("Anten: status requested.")

        async def _kill(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._admin(chat_id):
                await update.message.reply_text("Anten: admin only.")
                return
            self.bus.publish("risk/kill", {"on": True, "chat_id": chat_id})
            await update.message.reply_text("Anten: kill switch ON.")

        async def _unkill(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._admin(chat_id):
                await update.message.reply_text("Anten: admin only.")
                return
            self.bus.publish("risk/kill", {"on": False, "chat_id": chat_id})
            await update.message.reply_text("Anten: kill switch OFF.")

        async def _positions(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._chat_allowed(chat_id):
                await update.message.reply_text("Anten: access denied.")
                return
            self.bus.publish("tg/positions", {"chat_id": chat_id})
            await update.message.reply_text("Anten: positions requested.")

        async def _closeall(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._admin(chat_id):
                await update.message.reply_text("Anten: admin only.")
                return
            self.bus.publish("broker/close_all", {"reason": "telegram_closeall", "chat_id": chat_id})
            await update.message.reply_text("Anten: close_all requested.")

        async def _on_text(update: "Update", context: "ContextTypes.DEFAULT_TYPE") -> None:
            chat_id = update.effective_chat.id if update.effective_chat else 0
            if not self._chat_allowed(chat_id):
                return
            txt = (update.message.text or "").strip()
            if not txt:
                return
            if txt.startswith("{") and txt.endswith("}"):
                try:
                    raw = json.loads(txt)
                except Exception:
                    await update.message.reply_text("Anten: bad JSON.")
                    return
                self.bus.publish("signal/raw", {"raw": raw, "source": "telegram", "chat_id": chat_id})
                await update.message.reply_text("Anten: signal received.")
                return
            if txt.lower().startswith("price "):
                parts = txt.split()
                if len(parts) >= 3:
                    market = parts[1].strip().upper()
                    px = safe_float(parts[2], 0.0)
                    self.bus.publish("oracle/snapshot", {"market": market, "px": px, "chat_id": chat_id})
                    await update.message.reply_text(f"Anten: snapshot set for {market}.")
                    return
            await update.message.reply_text("Anten: unrecognized message. Use /help.")

        app = Application.builder().token(token).build()
        self._app = app

        app.add_handler(CommandHandler("start", _start))
        app.add_handler(CommandHandler("help", _help))
        app.add_handler(CommandHandler("status", _status))
        app.add_handler(CommandHandler("positions", _positions))
        app.add_handler(CommandHandler("closeall", _closeall))
        app.add_handler(CommandHandler("kill", _kill))
        app.add_handler(CommandHandler("unkill", _unkill))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), _on_text))

        LOG.info("Telegram: starting polling")
        try:
            app.run_polling(close_loop=False, stop_signals=None)
        except Exception as e:
            LOG.warn(f"Telegram: stopped ({e})")
        finally:
            self._app = None
