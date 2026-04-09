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
