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

    def _chat_allowed(self, chat_id: int) -> bool:
        if not self.cfg.telegram_chat_allowlist:
            return True
        return int(chat_id) in set(int(x) for x in self.cfg.telegram_chat_allowlist)

    def _admin(self, chat_id: int) -> bool:
        if int(chat_id) in set(int(x) for x in self.cfg.telegram_admins):
            return True
        # fallback: allow if allowlist exists and chat is allowlisted
        if self.cfg.telegram_admins:
            return False
        return self._chat_allowed(chat_id)


# ---- IMPORT / EXPORT / WATCHERS ----


class SignalIngestor:
    def __init__(self, codec: SignalCodec, bus: EventBus) -> None:
        self.codec = codec
        self.bus = bus

    def ingest_dict(self, raw: dict, source: str = "manual") -> Signal:
        s = self.codec.parse(raw)
        self.bus.publish("signal/parsed", {"signal_id": s.id, "market": s.market, "score": s.score(), "source": source})
        return s

    def ingest_json_text(self, text: str, source: str = "text") -> Signal:
        try:
            raw = json.loads(text)
        except Exception as e:
            raise SignalError(f"bad JSON: {e}") from e
        if not isinstance(raw, dict):
            raise SignalError("JSON must be an object")
        return self.ingest_dict(raw, source=source)

    def ingest_file(self, path: str, source: str = "file") -> list[Signal]:
        txt = open(path, "r", encoding="utf-8").read()
        txt = txt.strip()
        if not txt:
            return []
        out: list[Signal] = []
        if txt.startswith("["):
            arr = json.loads(txt)
            if not isinstance(arr, list):
                raise SignalError("expected list")
            for item in arr:
                if isinstance(item, dict):
                    out.append(self.ingest_dict(item, source=source))
            return out
        out.append(self.ingest_json_text(txt, source=source))
        return out

    def export_signals(self, store: SqliteStore, out_path: str, limit: int = 500) -> None:
        rows = store.list_signals(limit=limit)
        payload = {
            "app": APP_NAME,
            "version": APP_VERSION,
            "t": utc_ts(),
            "count": len(rows),
            "signals": rows,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)


class Web3WatcherStub:
    """Reads NDJSON lines from a file and publishes them as raw signals."""

    def __init__(self, bus: EventBus, path: str) -> None:
        self.bus = bus
        self.path = path
        self._stop = threading.Event()
        self._thr = threading.Thread(target=self._run, name="watcher", daemon=True)
        self._pos = 0

    def start(self) -> None:
        self._thr.start()

    def stop(self) -> None:
        self._stop.set()
        self._thr.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if not os.path.exists(self.path):
                    jitter_sleep(0.7)
                    continue
                with open(self.path, "r", encoding="utf-8") as f:
                    f.seek(self._pos)
                    chunk = f.read()
                    self._pos = f.tell()
                if not chunk:
                    jitter_sleep(0.35)
                    continue
                for line in chunk.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(raw, dict):
                        self.bus.publish("signal/raw", {"raw": raw, "source": "web3_stub"})
            except Exception as e:
                LOG.debug(f"watcher stub error: {e}")
                jitter_sleep(1.0)


# ---- UI ----


class UiModel:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.status_lines: list[str] = []
        self.last_signal: dict | None = None
        self.last_action: dict | None = None
        self.toast: str | None = None

    def push(self, line: str, cap: int = 400) -> None:
        with self._lock:
            self.status_lines.append(line)
            if len(self.status_lines) > cap:
                self.status_lines = self.status_lines[-cap:]

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "lines": list(self.status_lines),
                "last_signal": self.last_signal,
                "last_action": self.last_action,
                "toast": self.toast,
            }

    def set_toast(self, msg: str | None) -> None:
        with self._lock:
            self.toast = msg


class DesktopApp:
    def __init__(self, cfg: AppConfig, identity: AppIdentity, bus: EventBus, model: UiModel) -> None:
        if tk is None:
            raise RuntimeError("Tkinter is not available in this Python environment.")
        self.cfg = cfg
        self.identity = identity
        self.bus = bus
        self.model = model

        self.root = tk.Tk()
        self.root.title(f"{APP_NAME} — {APP_VERSION}")
        self.root.geometry("1080x720")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self._build_style()
        self._build_layout()

        self._ui_last_snapshot: dict | None = None
        self._tick()

    def _build_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        if self.cfg.theme == "dark":
            self.root.configure(bg="#101418")
            style.configure("TFrame", background="#101418")
            style.configure("TLabel", background="#101418", foreground="#e6edf3")
            style.configure("TButton", padding=6)
            style.configure("TEntry", padding=4)
            style.configure("TLabelframe", background="#101418", foreground="#e6edf3")
            style.configure("TLabelframe.Label", background="#101418", foreground="#e6edf3")
        else:
            self.root.configure(bg="#f3f6fb")

    def _build_layout(self) -> None:
        top = ttk.Frame(self.root)
        top.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)

        self.lbl_title = ttk.Label(top, text=f"{APP_NAME}  |  Run: {self.identity.run_id}")
        self.lbl_title.pack(side=tk.LEFT)

        self.btn_import = ttk.Button(top, text="Import signal JSON…", command=self.import_signal)
        self.btn_import.pack(side=tk.RIGHT, padx=6)

        self.btn_export = ttk.Button(top, text="Export signals…", command=self.export_signals)
        self.btn_export.pack(side=tk.RIGHT, padx=6)

        mid = ttk.Frame(self.root)
        mid.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        left = ttk.Frame(mid)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 8))

        right = ttk.Frame(mid)
        right.pack(side=tk.RIGHT, fill=tk.Y)

        # Status console
        lf = ttk.Labelframe(left, text="Live console")
        lf.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.txt = tk.Text(lf, height=20, wrap="word", bg="#0b0f14", fg="#e6edf3", insertbackground="#e6edf3")
        self.txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(lf, orient="vertical", command=self.txt.yview)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.txt.configure(yscrollcommand=sb.set)

        # Controls panel
        lf2 = ttk.Labelframe(right, text="Controls")
        lf2.pack(side=tk.TOP, fill=tk.X, pady=(0, 10))

        self.var_kill = tk.BooleanVar(value=bool(self.cfg.kill_switch))
        chk = ttk.Checkbutton(lf2, text="Kill switch", variable=self.var_kill, command=self.on_kill_toggle)
        chk.pack(side=tk.TOP, anchor="w", padx=8, pady=6)

        ttk.Button(lf2, text="Close all positions", command=self.on_close_all).pack(
            side=tk.TOP, fill=tk.X, padx=8, pady=4
        )
        ttk.Button(lf2, text="Snapshot equity", command=self.on_snapshot).pack(
            side=tk.TOP, fill=tk.X, padx=8, pady=4
        )

        # Signal composer
        lf3 = ttk.Labelframe(right, text="Quick signal")
        lf3.pack(side=tk.TOP, fill=tk.X)

        self.ent_market = ttk.Entry(lf3)
        self.ent_market.insert(0, "BTC:USDT")
        self.ent_market.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)

        self.ent_conf = ttk.Entry(lf3)
        self.ent_conf.insert(0, "0.72")
        self.ent_conf.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)

        row = ttk.Frame(lf3)
        row.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)
        ttk.Button(row, text="Long", command=lambda: self.send_quick_signal(1)).pack(side=tk.LEFT, expand=True, fill=tk.X)
        ttk.Button(row, text="Flat", command=lambda: self.send_quick_signal(0)).pack(side=tk.LEFT, expand=True, fill=tk.X)
        ttk.Button(row, text="Short", command=lambda: self.send_quick_signal(-1)).pack(side=tk.LEFT, expand=True, fill=tk.X)

        # Telemetry
        lf4 = ttk.Labelframe(right, text="Telemetry")
        lf4.pack(side=tk.TOP, fill=tk.X, pady=(10, 0))

        self.lbl_equity = ttk.Label(lf4, text="Equity: —")
        self.lbl_equity.pack(side=tk.TOP, anchor="w", padx=8, pady=4)
        self.lbl_cash = ttk.Label(lf4, text="Cash: —")
        self.lbl_cash.pack(side=tk.TOP, anchor="w", padx=8, pady=4)
        self.lbl_pos = ttk.Label(lf4, text="Positions: —")
        self.lbl_pos.pack(side=tk.TOP, anchor="w", padx=8, pady=4)
        self.lbl_last = ttk.Label(lf4, text="Last: —")
        self.lbl_last.pack(side=tk.TOP, anchor="w", padx=8, pady=4)

    def _tick(self) -> None:
        snap = self.model.snapshot()
        if self._ui_last_snapshot != snap:
            self._render(snap)
            self._ui_last_snapshot = snap
        self.root.after(int(self.cfg.ui_tick_ms), self._tick)

    def _render(self, snap: dict) -> None:
        lines: list[str] = snap["lines"]
        if lines:
            self.txt.delete("1.0", "end")
            self.txt.insert("end", "\n".join(lines[-350:]) + "\n")
            self.txt.see("end")
        last_signal = snap.get("last_signal")
        if last_signal:
            self.lbl_last.configure(text=f"Last: {last_signal.get('market')} conf={last_signal.get('confidence')}")

        toast = snap.get("toast")
        if toast:
            self.root.title(f"{APP_NAME} — {toast}")

    def set_telemetry(self, equity: float, cash: float, npos: int) -> None:
        self.lbl_equity.configure(text=f"Equity: {fmt_money(equity)}")
        self.lbl_cash.configure(text=f"Cash: {fmt_money(cash)}")
        self.lbl_pos.configure(text=f"Positions: {npos}")

    def on_kill_toggle(self) -> None:
        on = bool(self.var_kill.get())
        self.bus.publish("risk/kill", {"on": on, "source": "ui"})

    def on_close_all(self) -> None:
        self.bus.publish("broker/close_all", {"reason": "ui_closeall"})

    def on_snapshot(self) -> None:
        self.bus.publish("ui/snapshot", {})

    def send_quick_signal(self, direction: int) -> None:
        market = self.ent_market.get().strip().upper()
        conf = safe_float(self.ent_conf.get().strip(), 0.65)
        raw = {
            "market": market,
            "strat": "ui/quick",
            "direction": int(direction),
            "confidence": float(conf),
            "notional_hint": 0.0,
            "meta_hash": sha256_hex(f"ui|{utc_ts()}|{market}|{direction}|{conf}")[:64],
        }
        self.bus.publish("signal/raw", {"raw": raw, "source": "ui"})

    def import_signal(self) -> None:
        if filedialog is None:
            return
        path = filedialog.askopenfilename(
            title="Import signal JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return
        self.bus.publish("io/import_file", {"path": path})

    def export_signals(self) -> None:
        if filedialog is None:
            return
        path = filedialog.asksaveasfilename(
            title="Export signals",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return
        self.bus.publish("io/export_signals", {"path": path})

    def on_close(self) -> None:
        self.bus.publish("app/stop", {"source": "ui"})

    def run(self) -> None:
        self.root.mainloop()


# ---- APP ORCHESTRATION ----


class AntenRuntime:
    def __init__(self) -> None:
        self.identity = AppIdentity(run_id=short_id("run_"), instance_salt=secrets.token_hex(24))
        self.cfg_store = ConfigStore(os.path.join(app_home(), "config.json"))
        self.cfg = self.cfg_store.load()
        self.cfg.normalize()

        self.store = SqliteStore(self.cfg.db_path)
        self.bus = EventBus()
        self.model = UiModel()
        self.codec = SignalCodec(self.identity)
        self.limiter = SignalLimiter(self.cfg.max_signals_per_min)
        self.oracle = PriceOracle(self.identity)
        self.risk = RiskEngine(self.cfg)
        self.broker = PaperBroker(self.cfg, self.oracle, self.store, self.risk)
        self.router = StrategyRouter(self.cfg, self.limiter, self.broker, self.store)
        self.ingestor = SignalIngestor(self.codec, self.bus)
        self.telegram = TelegramBotRunner(self.cfg, self.bus)

        self.desktop: DesktopApp | None = None
        self.watcher: Web3WatcherStub | None = None

        self._stop = threading.Event()
        self._bg_threads: list[threading.Thread] = []

        self._wire_events()

    def _wire_events(self) -> None:
        self.bus.subscribe("signal/raw", self._on_signal_raw)
        self.bus.subscribe("io/import_file", self._on_import_file)
        self.bus.subscribe("io/export_signals", self._on_export_signals)
        self.bus.subscribe("oracle/snapshot", self._on_oracle_snapshot)
        self.bus.subscribe("risk/kill", self._on_risk_kill)
        self.bus.subscribe("broker/close_all", self._on_close_all)
        self.bus.subscribe("ui/snapshot", self._on_ui_snapshot)
        self.bus.subscribe("tg/status", self._on_tg_status)
        self.bus.subscribe("tg/positions", self._on_tg_positions)
        self.bus.subscribe("app/stop", self._on_stop)

    def start(self) -> None:
        LOG.info(f"{APP_NAME} starting (version={APP_VERSION}, run={self.identity.run_id})")
        self.model.push(f"{APP_NAME} {APP_VERSION} — {human_dt(utc_ts())}")
        self.model.push(f"db: {self.cfg.db_path}")
        self.model.push(f"export: {self.cfg.export_dir}")
        self.model.push("paper trading: ON")

        self.bus.start()

        # start background telemetry pump
        thr = threading.Thread(target=self._telemetry_loop, name="telemetry", daemon=True)
        thr.start()
        self._bg_threads.append(thr)

        # optional watcher stub file
        stub_path = os.path.join(app_home(), "web3_events.ndjson")
        self.watcher = Web3WatcherStub(self.bus, stub_path)
        self.watcher.start()
        self.model.push(f"watcher stub: {stub_path}")

        # optional telegram
        self.telegram.start()

        if tk is not None:
            self.desktop = DesktopApp(self.cfg, self.identity, self.bus, self.model)
        else:
            self.desktop = None

    def run(self) -> None:
        self.start()
        if self.desktop is not None:
            self.desktop.run()
        else:
            self._run_headless()

    def _run_headless(self) -> None:
        self.model.push("UI disabled: running headless. Press Ctrl+C to exit.")
        try:
            while not self._stop.is_set():
                jitter_sleep(0.25)
        except KeyboardInterrupt:
            self._stop.set()
        finally:
            self.stop()

    def stop(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        LOG.info("Stopping...")
        try:
            if self.watcher:
                self.watcher.stop()
        except Exception:
            pass
        try:
            self.telegram.stop()
        except Exception:
            pass
        try:
            self.bus.stop()
        except Exception:
            pass
        try:
            self.store.close()
        except Exception:
            pass
        try:
            self.cfg_store.save(self.cfg)
        except Exception:
            pass

    # ---------------------
    # Event handlers
    # ---------------------

    def _on_signal_raw(self, ev: Event) -> None:
        raw = ev.payload.get("raw")
        source = ev.payload.get("source", "unknown")
        try:
            sig = self.ingestor.ingest_dict(raw, source=str(source))
            self.model.last_signal = {
                "id": sig.id,
                "market": sig.market,
                "confidence": sig.confidence,
                "direction": sig.direction,
                "t": sig.t,
                "source": source,
            }
            self.model.push(f"signal {sig.market} dir={sig.direction} conf={sig.confidence:.2f} src={source}")

            action = self.router.on_signal(sig)
            self.model.last_action = action
            if action.get("status") == "opened":
                self.model.push(f"trade OPEN {action.get('market')} side={action.get('side')} id={action.get('trade_id')}")
                self._tg_broadcast(f"OPEN {action.get('market')} side={action.get('side')} (paper)")
