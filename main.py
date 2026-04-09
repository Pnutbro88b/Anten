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
