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
