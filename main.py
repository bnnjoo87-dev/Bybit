#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit Multi-Strategy Trading Bot - Reference Implementation
Sections map to the original spec. Runs 4 isolated profiles in one process.
"""

# ════════════════════════════════════════════════════════════════
# SECTION 0: IMPORTS
# ════════════════════════════════════════════════════════════════
import os, sys, re, json, time, hmac, hashlib, threading, traceback, argparse
import sqlite3, importlib.util, shutil, math, socket
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import requests
import pandas as pd
import numpy as np

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ════════════════════════════════════════════════════════════════
# SECTION 1: SETTINGS
# ════════════════════════════════════════════════════════════════

APP_START_TS = int(time.time() * 1000)

def _clean_secret(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.strip().strip('"').strip("'").replace("\u200b", "")

class Settings:
    def __init__(self):
        self.bybit_api_key    = _clean_secret(os.getenv("BYBIT_API_KEY"))
        self.bybit_api_secret = _clean_secret(os.getenv("BYBIT_API_SECRET"))
        self.testnet          = os.getenv("BYBIT_TESTNET", "false").lower() == "true"
        self.tg_token         = _clean_secret(os.getenv("TELEGRAM_BOT_TOKEN"))
        self.tg_chat_id       = _clean_secret(os.getenv("TELEGRAM_CHAT_ID"))
        self.allowed_users    = [u.strip() for u in (os.getenv("ALLOWED_TELEGRAM_USER_IDS") or "").split(",") if u.strip()]

        self.port             = int(os.getenv("PORT", "8080"))
        self.log_level        = os.getenv("LOG_LEVEL", "INFO").upper()
        self.poll_interval    = int(os.getenv("POLL_INTERVAL_SEC", "5"))
        self.data_root        = os.getenv("DATA_ROOT", "data")

        # Risk defaults (per profile, overridable via runtime_overrides)
        self.risk_per_trade_pct = float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))
        self.max_open_positions = int(os.getenv("MAX_OPEN_POSITIONS", "1"))
        self.max_daily_loss_pct = float(os.getenv("MAX_DAILY_LOSS_PCT", "3.0"))
        self.leverage           = int(os.getenv("LEVERAGE", "5"))
        self.min_rr             = float(os.getenv("MIN_RR", "1.35"))
        self.cooldown_sec       = int(os.getenv("COOLDOWN_SEC", "300"))

        # SL/TP defaults
        self.sl_percent         = float(os.getenv("SL_PERCENT", "0.8"))
        self.tp_percent         = float(os.getenv("TP_PERCENT", "1.6"))

        # Symbols
        default_syms = "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT,LINKUSDT,DOTUSDT"
        self.symbols = [s.strip().upper() for s in os.getenv("SYMBOLS", default_syms).split(",") if s.strip()]
        extra = os.getenv("EXTRA_SYMBOLS", "")
        if extra:
            self.symbols += [s.strip().upper() for s in extra.split(",") if s.strip()]

    def validate(self) -> List[str]:
        errs = []
        if not self.bybit_api_key or not self.bybit_api_secret:
            errs.append("BYBIT_API_KEY / BYBIT_API_SECRET missing")
        if not self.tg_token or not self.tg_chat_id:
            errs.append("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing")
        if self.leverage < 1 or self.leverage > 125:
            errs.append("LEVERAGE must be 1..125")
        return errs

# ════════════════════════════════════════════════════════════════
# SECTION 2: LOGGING
# ════════════════════════════════════════════════════════════════

LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}

class TechErrorLogger:
    """Per-profile + global technical logger with dedupe."""
    def __init__(self, path: str, profile_id: int = 0):
        self.path = path
        self.pid = profile_id
        self._last_ts = {}
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def log(self, component: str, func: str, symbol: str, exc: Exception,
            trade_uid: str = "", mode: str = "") -> bool:
        key = (component, func, type(exc).__name__, str(exc)[:120])
        now = time.time()
        with self._lock:
            if now - self._last_ts.get(key, 0) < 60:
                return False
            self._last_ts[key] = now
            line = json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "pid": self.pid, "component": component, "func": func,
                "symbol": symbol, "trade_uid": trade_uid, "mode": mode,
                "exc_type": type(exc).__name__, "msg": str(exc)[:500],
                "tb": traceback.format_exc()[-2000:],
            }, ensure_ascii=False)
            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except Exception:
                pass
        return True

def console_log(level: str, msg: str):
    if LOG_LEVELS.get(level, 20) >= LOG_LEVELS.get(SETTINGS.log_level, 20):
        print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] {level:5s} {msg}", flush=True)

# ════════════════════════════════════════════════════════════════
# SECTION 3: HEALTH SERVER
# ════════════════════════════════════════════════════════════════

GLOBAL = {"active_profile": 1, "live_owner": None, "ready": False}

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            "status": "ok" if GLOBAL["ready"] else "starting",
            "uptime_sec": int(time.time() - APP_START_TS/1000),
            "active_profile": GLOBAL["active_profile"],
            "live_owner": GLOBAL["live_owner"],
            "pid": os.getpid(),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a, **k): pass

def start_health_server(port: int):
    srv = HTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=srv.serve_forever, daemon=True, name="health").start()
    console_log("INFO", f"Health server on :{port}")

# ════════════════════════════════════════════════════════════════
# SECTION 4: TELEGRAM OBSERVER (raw Bot API - no external dep)
# ════════════════════════════════════════════════════════════════

class TelegramObserver:
    BASE = "https://api.telegram.org/bot{token}/{method}"
    def __init__(self, token: str, chat_id: str, allowed_users: List[str], errors: TechErrorLogger):
        self.token = token
        self.chat_id = chat_id
        self.allowed = set(allowed_users)
        self.errors = errors
        self.send_q: "List[dict]" = []
        self.lock = threading.Lock()
        self._offset = 0
        self.commands: Dict[str, Callable] = {}
        self.doc_handler: Optional[Callable] = None
        self.running = True

    def _url(self, m): return self.BASE.format(token=self.token, method=m)

    def send(self, text: str, reply_markup: Optional[dict] = None) -> bool:
        try:
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML",
                       "disable_web_page_preview": True}
            if reply_markup:
                payload["reply_markup"] = json.dumps(reply_markup)
            r = requests.post(self._url("sendMessage"), json=payload, timeout=10)
            return r.ok
        except Exception as e:
            self.errors.log("TELEGRAM", "send", "", e)
            return False

    def reply(self, chat_id: Any, text: str, markup: Optional[dict] = None):
        try:
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
                       "disable_web_page_preview": True}
            if markup:
                payload["reply_markup"] = json.dumps(markup)
            requests.post(self._url("sendMessage"), json=payload, timeout=10)
        except Exception as e:
            self.errors.log("TELEGRAM", "reply", "", e)

    def register(self, cmd: str, fn: Callable[[str, dict], None]):
        self.commands[cmd] = fn

    def start_polling(self):
        try:
            requests.get(self._url("deleteWebhook"), timeout=10)
        except Exception: pass
        threading.Thread(target=self._loop, daemon=True, name="tg-poll").start()

    def _loop(self):
        backoff = 1
        while self.running:
            try:
                r = requests.get(self._url("getUpdates"),
                                 params={"offset": self._offset, "timeout": 25},
                                 timeout=35)
                if not r.ok:
                    time.sleep(backoff); backoff = min(backoff*2, 30); continue
                backoff = 1
                for u in r.json().get("result", []):
                    self._offset = u["update_id"] + 1
                    self._dispatch(u)
            except Exception as e:
                self.errors.log("TELEGRAM", "poll", "", e)
                time.sleep(backoff); backoff = min(backoff*2, 30)

    def _is_allowed(self, user_id: Any) -> bool:
        if not self.allowed: return True  # dev mode
        return str(user_id) in self.allowed

    def _dispatch(self, update: dict):
        try:
            msg = update.get("message") or {}
            user_id = (msg.get("from") or {}).get("id")
            chat_id = msg.get("chat", {}).get("id")
            if not self._is_allowed(user_id):
                self.reply(chat_id, "⛔ Unauthorized.")
                return
            text = msg.get("text", "") or ""
            if text.startswith("/"):
                parts = text.split()
                cmd = parts[0].split("@")[0].lstrip("/")
                handler = self.commands.get(cmd) or self.commands.get("help")
                if handler:
                    handler(chat_id, {"args": parts[1:], "raw": text, "user_id": user_id})
            elif "document" in msg:
                if self.doc_handler:
                    self.doc_handler(chat_id, msg["document"], user_id)
        except Exception as e:
            self.errors.log("TELEGRAM", "dispatch", "", e)

# ════════════════════════════════════════════════════════════════
# SECTION 5: BYBIT CLIENT (V5, HMAC-SHA256)
# ════════════════════════════════════════════════════════════════

class BybitClient:
    def __init__(self, key: str, secret: str, testnet: bool, errors: TechErrorLogger):
        self.key = key
        self.secret = secret.encode() if secret else b""
        self.base = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        self.errors = errors
        self.recv_window = "5000"
        self.session = requests.Session()

    def _sign(self, ts: str, payload: str) -> str:
        return hmac.new(self.secret, (ts + self.key + self.recv_window + payload).encode(),
                        hashlib.sha256).hexdigest()

    def _request(self, method: str, path: str, params: Optional[dict] = None,
                 body: Optional[dict] = None, signed: bool = False) -> dict:
        ts = str(int(time.time() * 1000))
        url = self.base + path
        headers = {"Content-Type": "application/json"}
        qs = ""
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
        payload_str = ""
        data = None
        if method == "POST":
            payload_str = json.dumps(body or {}, separators=(",", ":"))
            data = payload_str
        elif params:
            payload_str = qs
        if signed:
            headers["X-BAPI-API-KEY"] = self.key
            headers["X-BAPI-TIMESTAMP"] = ts
            headers["X-BAPI-RECV-WINDOW"] = self.recv_window
            headers["X-BAPI-SIGN"] = self._sign(ts, payload_str)
        try:
            if method == "GET":
                r = self.session.get(url + ("?" + qs if qs else ""), headers=headers, timeout=10)
            else:
                r = self.session.post(url, headers=headers, data=data, timeout=10)
            j = r.json()
            if j.get("retCode") not in (0, None):
                raise RuntimeError(f"Bybit {path} retCode={j.get('retCode')} msg={j.get('retMsg')}")
            return j.get("result", {})
        except Exception as e:
            self.errors.log("BYBIT", path, "", e)
            raise

    # ---- Public helpers ----
    def kline(self, symbol: str, interval: str = "5", limit: int = 200) -> pd.DataFrame:
        res = self._request("GET", "/v5/market/kline",
                            params={"category": "linear", "symbol": symbol,
                                    "interval": interval, "limit": limit})
        rows = res.get("list", [])[::-1]  # oldest first
        df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","turnover"])
        for c in ["open","high","low","close","volume","turnover"]:
            df[c] = df[c].astype(float)
        df["open_time"] = df["open_time"].astype("int64")
        return df

    def ticker(self, symbol: str) -> dict:
        res = self._request("GET", "/v5/market/tickers",
                            params={"category": "linear", "symbol": symbol})
        rows = res.get("list", [])
        return rows[0] if rows else {}

    def instruments_info(self, symbol: str) -> dict:
        res = self._request("GET", "/v5/market/instruments-info",
                            params={"category": "linear", "symbol": symbol})
        rows = res.get("list", [])
        return rows[0] if rows else {}

    # ---- Signed helpers ----
    def wallet_balance(self) -> dict:
        return self._request("GET", "/v5/account/wallet-balance",
                             params={"accountType": "UNIFIED"}, signed=True)

    def positions(self, symbol: Optional[str] = None) -> List[dict]:
        p = {"category": "linear"}
        if symbol: p["symbol"] = symbol
        res = self._request("GET", "/v5/position/list", params=p, signed=True)
        return res.get("list", [])

    def set_leverage(self, symbol: str, lev: int):
        return self._request("POST", "/v5/position/set-leverage",
                             body={"category": "linear", "symbol": symbol,
                                   "buyLeverage": str(lev), "sellLeverage": str(lev)},
                             signed=True)

    def create_order(self, symbol: str, side: str, qty: str,
                     order_type: str = "Market", reduce_only: bool = False) -> dict:
        body = {"category": "linear", "symbol": symbol, "side": side,
                "orderType": order_type, "qty": qty, "reduceOnly": reduce_only}
        return self._request("POST", "/v5/order/create", body=body, signed=True)

    def set_trading_stop(self, symbol: str, stop_loss: Optional[str] = None,
                         take_profit: Optional[str] = None, position_idx: int = 0):
        body = {"category": "linear", "symbol": symbol, "positionIdx": position_idx}
        if stop_loss:   body["stopLoss"] = stop_loss
        if take_profit: body["takeProfit"] = take_profit
        return self._request("POST", "/v5/position/trading-stop", body=body, signed=True)

    def public_ip(self) -> str:
        try:
            return requests.get("https://api.ipify.org", timeout=8).text.strip()
        except Exception:
            return "unknown"

# ════════════════════════════════════════════════════════════════
# SECTION 6: SYMBOL RULES
# ════════════════════════════════════════════════════════════════

@dataclass
class SymbolRules:
    symbol: str
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float

    @classmethod
    def from_bybit(cls, symbol: str, info: dict) -> "SymbolRules":
        lot = info.get("lotSizeFilter", {})
        prc = info.get("priceFilter", {})
        return cls(
            symbol=symbol,
            tick_size=float(prc.get("tickSize", "0.01")),
            step_size=float(lot.get("qtyStep", "0.001")),
            min_qty=float(lot.get("minOrderQty", "0")),
            min_notional=float(lot.get("minNotionalValue", "5")),
        )

    def round_price(self, p: float) -> float:
        if self.tick_size <= 0: return float(p)
        d = Decimal(str(p)) / Decimal(str(self.tick_size))
        return float(d.to_integral_value(rounding=ROUND_DOWN) * Decimal(str(self.tick_size)))

    def round_qty(self, q: float) -> float:
        if self.step_size <= 0: return float(q)
        d = Decimal(str(q)) / Decimal(str(self.step_size))
        return float(d.to_integral_value(rounding=ROUND_DOWN) * Decimal(str(self.step_size)))

class SymbolRulesCache:
    def __init__(self, client: BybitClient):
        self.client = client
        self._rules: Dict[str, SymbolRules] = {}
        self._lock = threading.Lock()

    def get(self, symbol: str) -> SymbolRules:
        with self._lock:
            if symbol in self._rules:
                return self._rules[symbol]
        info = self.client.instruments_info(symbol)
        if not info:
            raise RuntimeError(f"No instrument info for {symbol}")
        rules = SymbolRules.from_bybit(symbol, info)
        with self._lock:
            self._rules[symbol] = rules
        return rules

# ════════════════════════════════════════════════════════════════
# SECTION 7: STRATEGY MANAGER
# ════════════════════════════════════════════════════════════════

@dataclass
class StrategyContext:
    symbol: str
    timeframe: str
    now_ms: int
    last_price: float
    df: pd.DataFrame
    data: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    config: dict = field(default_factory=dict)
    signal_key: str = ""

class StrategyManager:
    """Atomic hot-reload, LKG fallback, rollback."""
    def __init__(self, profile_id: int, base_dir: str, errors: TechErrorLogger):
        self.pid = profile_id
        self.base_dir = base_dir
        self.errors = errors
        self.primary_path = os.path.join(base_dir, "strategy.py")
        self.lkg_path = os.path.join(base_dir, "strategy.last_good.py")
        self.backups_dir = os.path.join(base_dir, "strategy_backups")
        os.makedirs(self.backups_dir, exist_ok=True)
        self.module_name = f"user_strategy_p{profile_id}"
        self._lock = threading.Lock()
        self._mod = None
        self.loaded_from = "none"
        self.name = "NONE"
        self.version = "0.0.0"
        self.file_sha = ""

    def _load_from_path(self, path: str) -> Any:
        spec = importlib.util.spec_from_file_location(self.module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Cannot load {path}")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[self.module_name] = mod
        spec.loader.exec_module(mod)
        if not hasattr(mod, "run_strategy"):
            raise RuntimeError("run_strategy missing")
        return mod

    def _dummy_test(self, mod) -> None:
        df = pd.DataFrame({
            "open_time": [int(time.time()*1000) - i*300000 for i in range(60, 0, -1)],
            "open": [100.0]*60, "high": [101.0]*60, "low": [99.0]*60,
            "close": [100.5]*60, "volume": [1000.0]*60,
        })
        ctx = StrategyContext(symbol="BTCUSDT", timeframe="5m",
                              now_ms=int(time.time()*1000), last_price=100.5,
                              df=df, config=getattr(mod, "STRATEGY_CONFIG", {}))
        out = None
        try:
            if len(__import__("inspect").signature(mod.run_strategy).parameters) == 1:
                out = mod.run_strategy(ctx)
            else:
                out = mod.run_strategy(df, "BTCUSDT", {"now_ms": ctx.now_ms, "last_price": 100.5,
                                                       "context": ctx, "data": {}},
                                       getattr(mod, "STRATEGY_CONFIG", {}))
        except Exception as e:
            raise RuntimeError(f"dummy test failed: {e}")
        if not isinstance(out, dict) or "action" not in out:
            raise RuntimeError("run_strategy must return dict with action")

    def load(self) -> bool:
        with self._lock:
            for path, src in ((self.primary_path, "primary"), (self.lkg_path, "lkg")):
                if not os.path.exists(path): continue
                try:
                    mod = self._load_from_path(path)
                    self._dummy_test(mod)
                    self._mod = mod
                    self.loaded_from = src
                    self.name = getattr(mod, "STRATEGY_NAME", "Unnamed")
                    self.version = getattr(mod, "STRATEGY_VERSION", "1.0.0")
                    self.file_sha = self._sha(path)
                    return True
                except Exception as e:
                    self.errors.log("STRATEGY", f"load[{src}]", "", e)
            self._mod = None
            self.loaded_from = "none"
            return False

    def _sha(self, path: str) -> str:
        try:
            with open(path, "rb") as f: return hashlib.sha256(f.read()).hexdigest()[:12]
        except Exception: return ""

    def replace(self, new_bytes: bytes) -> Dict[str, Any]:
        """Atomic replace with validation. Returns {ok, msg}."""
        tmp = self.primary_path + ".new"
        try:
            with open(tmp, "wb") as f: f.write(new_bytes)
            mod = self._load_from_path(tmp)
            self._dummy_test(mod)
        except Exception as e:
            try: os.remove(tmp)
            except Exception: pass
            return {"ok": False, "msg": f"Validation failed: {e}"}
        # Backup current
        if os.path.exists(self.primary_path):
            bak = os.path.join(self.backups_dir,
                               f"strategy_p{self.pid}_{int(time.time())}_{self.file_sha or 'x'}.py")
            try: shutil.copy2(self.primary_path, bak)
            except Exception: pass
            try: shutil.copy2(self.primary_path, self.lkg_path)
            except Exception: pass
        os.replace(tmp, self.primary_path)
        return {"ok": True, "msg": "Strategy replaced", **self.load() and {}}

    def rollback(self) -> Dict[str, Any]:
        backups = sorted([f for f in os.listdir(self.backups_dir) if f.endswith(".py")],
                         reverse=True)
        if not backups:
            return {"ok": False, "msg": "No backups"}
        src = os.path.join(self.backups_dir, backups[0])
        try:
            shutil.copy2(src, self.primary_path)
            if self.load(): return {"ok": True, "msg": f"Rolled back to {backups[0]}"}
        except Exception as e:
            return {"ok": False, "msg": str(e)}
        return {"ok": False, "msg": "Rollback load failed"}

    def call(self, ctx: StrategyContext) -> dict:
        if not self._mod:
            return {"action": "NO_TRADE", "reason": "No strategy loaded"}
        try:
            import inspect
            if len(inspect.signature(self._mod.run_strategy).parameters) == 1:
                out = self._mod.run_strategy(ctx)
            else:
                out = self._mod.run_strategy(
                    ctx.df, ctx.symbol,
                    {"now_ms": ctx.now_ms, "last_price": ctx.last_price,
                     "context": ctx, "data": ctx.data},
                    ctx.config,
                )
            if not isinstance(out, dict):
                raise RuntimeError("non-dict returned")
            return out
        except Exception as e:
            self.errors.log("STRATEGY", "call", ctx.symbol, e)
            return {"action": "NO_TRADE", "reason": f"error: {e}"}

# ════════════════════════════════════════════════════════════════
# SECTION 4.1: RISK / SIZING
# ════════════════════════════════════════════════════════════════

def size_from_risk(equity: float, risk_pct: float, entry: float, stop: float,
                   rules: SymbolRules, scale_factor: float = 1.0,
                   avail_margin: float = 0.0, leverage: int = 1,
                   remaining_slots: int = 1) -> float:
    if entry <= 0 or stop <= 0 or abs(entry - stop) < 1e-9:
        return 0.0
    risk_amt = equity * (risk_pct / 100.0) * scale_factor
    dist = abs(entry - stop)
    raw_qty = risk_amt / dist
    slots = max(1, remaining_slots)
    slot_margin = (avail_margin * 0.90) / slots
    max_qty = (slot_margin * leverage) / entry if entry > 0 else 0
    raw_qty = min(raw_qty, max_qty) if max_qty > 0 else raw_qty
    qty = rules.round_qty(raw_qty)
    if qty < rules.min_qty or qty * entry < rules.min_notional:
        min_viable = max(rules.min_qty, (rules.min_notional * 1.02) / entry + rules.step_size)
        if (min_viable * entry) / max(1, leverage) <= slot_margin:
            qty = rules.round_qty(min_viable)
    return qty

# ════════════════════════════════════════════════════════════════
# SECTION 6.1: DATABASES (per-profile)
# ════════════════════════════════════════════════════════════════

def _open_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_state_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        trade_uid TEXT PRIMARY KEY,
        symbol TEXT, side TEXT, entry_price REAL, quantity REAL,
        sl_price REAL, tp_price REAL, status TEXT,
        opened_at INTEGER, closed_at INTEGER, exit_price REAL,
        realized_pnl REAL, is_shadow INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS trade_lifecycle (
        trade_uid TEXT PRIMARY KEY,
        entry_sent_ms INTEGER, entry_filled_ms INTEGER, exit_ms INTEGER,
        pnl_gross REAL, pnl_net REAL, fees REAL, progress_25_ms INTEGER,
        last_update_ms INTEGER
    );
    CREATE TABLE IF NOT EXISTS signals (
        signal_id TEXT PRIMARY KEY, symbol TEXT, side TEXT,
        strategy_name TEXT, signal_time_ms INTEGER,
        reference_price REAL, confidence REAL, reason TEXT
    );
    CREATE TABLE IF NOT EXISTS lifecycle_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_uid TEXT, event_type TEXT, dedupe_key TEXT UNIQUE,
        ts_ms INTEGER, payload TEXT
    );
    CREATE TABLE IF NOT EXISTS runtime_overrides (
        key TEXT PRIMARY KEY, value TEXT, updated_ms INTEGER
    );
    CREATE TABLE IF NOT EXISTS strategy_versions (
        strategy_id TEXT PRIMARY KEY, name TEXT, version TEXT,
        file_sha256 TEXT, first_seen_ms INTEGER
    );
    """)
    conn.commit()

def init_advisor_db(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS trade_progress_memory (
        trade_uid TEXT PRIMARY KEY,
        peak_progress_pct REAL DEFAULT 0,
        last_update_ms INTEGER
    );
    CREATE TABLE IF NOT EXISTS indicator_progress_profile (
        indicator TEXT PRIMARY KEY,
        safe_threshold_pct REAL DEFAULT 70,
        sample_count INTEGER DEFAULT 0,
        last_update_ms INTEGER
    );
    """)
    conn.commit()

def init_vault_db(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS indicator_trade_records (
        trade_uid TEXT PRIMARY KEY, symbol TEXT, side TEXT, mode TEXT,
        strategy_name TEXT, indicators_json TEXT, raw_reason TEXT,
        entry_price REAL, exit_price REAL, pnl REAL,
        is_closed INTEGER DEFAULT 0, exit_reason TEXT,
        entry_ms INTEGER, exit_ms INTEGER
    );
    CREATE TABLE IF NOT EXISTS indicator_stats (
        indicator TEXT PRIMARY KEY, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
        net_pnl REAL DEFAULT 0, expectancy REAL DEFAULT 0,
        max_loss_streak INTEGER DEFAULT 0, last_update_ms INTEGER
    );
    """)
    conn.commit()

# ════════════════════════════════════════════════════════════════
# SECTION 3.x: RUNTIME STATE + PROFILE
# ════════════════════════════════════════════════════════════════

@dataclass
class RuntimeState:
    profile_id: int
    mode: str = "SHADOW"  # SHADOW | LIVE
    timeframe: str = "5m"
    scale_factor: float = 1.0
    max_open_positions: int = 1
    leverage: int = 5
    sl_percent: float = 0.8
    tp_percent: float = 1.6
    last_entry_ms: Dict[str, int] = field(default_factory=dict)
    last_signal_key: Dict[str, str] = field(default_factory=dict)
    daily_pnl: float = 0.0
    daily_anchor_ms: int = 0

class Profile:
    def __init__(self, pid: int, base_dir: str, settings: Settings,
                 bybit: BybitClient, rules_cache: SymbolRulesCache, tg: TelegramObserver):
        self.pid = pid
        self.base_dir = base_dir
        self.settings = settings
        self.bybit = bybit
        self.rules = rules_cache
        self.tg = tg
        os.makedirs(base_dir, exist_ok=True)
        os.makedirs(os.path.join(base_dir, "vault"), exist_ok=True)

        self.errors = TechErrorLogger(os.path.join(base_dir, "errors.log"), pid)
        self.state_db = _open_db(os.path.join(base_dir, "state.db"))
        self.advisor_db = _open_db(os.path.join(base_dir, "advisor.db"))
        self.vault_db = _open_db(os.path.join(base_dir, "vault", "indicators_brain.db"))
        init_state_db(self.state_db)
        init_advisor_db(self.advisor_db)
        init_vault_db(self.vault_db)

        self.runtime = RuntimeState(profile_id=pid,
                                    leverage=settings.leverage,
                                    max_open_positions=settings.max_open_positions,
                                    sl_percent=settings.sl_percent,
                                    tp_percent=settings.tp_percent)
        self.strategy = StrategyManager(pid, base_dir, self.errors)

    def open_trades_count(self) -> int:
        row = self.state_db.execute(
            "SELECT COUNT(*) c FROM trades WHERE status='OPEN'").fetchone()
        return int(row["c"]) if row else 0

    def symbol_has_open(self, symbol: str) -> bool:
        row = self.state_db.execute(
            "SELECT 1 FROM trades WHERE symbol=? AND status='OPEN' LIMIT 1",
            (symbol,)).fetchone()
        return row is not None

    def record_signal(self, sig: dict):
        try:
            self.state_db.execute(
                """INSERT OR IGNORE INTO signals
                   (signal_id,symbol,side,strategy_name,signal_time_ms,
                    reference_price,confidence,reason)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (sig["signal_id"], sig["symbol"], sig["side"], sig["strategy"],
                 sig["ts"], sig["ref"], sig.get("confidence", 0), sig.get("reason", "")))
            self.state_db.commit()
        except Exception as e:
            self.errors.log("DB", "record_signal", sig.get("symbol", ""), e)

    def record_trade_open(self, trade_uid: str, symbol: str, side: str,
                          entry: float, qty: float, sl: float, tp: float, is_shadow: bool):
        try:
            self.state_db.execute(
                """INSERT OR REPLACE INTO trades
                   (trade_uid,symbol,side,entry_price,quantity,sl_price,tp_price,
                    status,opened_at,is_shadow)
                   VALUES (?,?,?,?,?,?,?, 'OPEN', ?, ?)""",
                (trade_uid, symbol, side, entry, qty, sl, tp,
                 int(time.time()*1000), 1 if is_shadow else 0))
            self.state_db.execute(
                "INSERT OR REPLACE INTO trade_lifecycle (trade_uid, entry_filled_ms, last_update_ms) VALUES (?,?,?)",
                (trade_uid, int(time.time()*1000), int(time.time()*1000)))
            self.state_db.commit()
        except Exception as e:
            self.errors.log("DB", "record_trade_open", symbol, e, trade_uid)

    def vault_record_entry(self, trade_uid: str, symbol: str, side: str, mode: str,
                           strategy_name: str, indicators: dict, reason: str, entry: float):
        try:
            self.vault_db.execute(
                """INSERT OR REPLACE INTO indicator_trade_records
                   (trade_uid,symbol,side,mode,strategy_name,indicators_json,
                    raw_reason,entry_price,is_closed,entry_ms)
                   VALUES (?,?,?,?,?,?,?,?,0,?)""",
                (trade_uid, symbol, side, mode, strategy_name,
                 json.dumps(indicators, ensure_ascii=False), reason, entry,
                 int(time.time()*1000)))
            self.vault_db.commit()
        except Exception as e:
            self.errors.log("VAULT", "record_entry", symbol, e, trade_uid)

    # ─── TRADE EXECUTION ───────────────────────────────────────────
    def execute_signal(self, symbol: str, sig: dict) -> Optional[str]:
        """Returns trade_uid on success, None on failure."""
        try:
            action = sig.get("action")
            if action not in ("LONG", "SHORT"):
                return None
            side = "Buy" if action == "LONG" else "Sell"
            entry = float(sig.get("reference_price") or 0)
            if entry <= 0:
                self.errors.log("EXEC", "execute_signal", symbol,
                                RuntimeError("invalid entry price"))
                return None
            rules = self.rules.get(symbol)
            equity, avail = self._equity_and_avail()
            if equity <= 0:
                return None
            sl = float(sig.get("stop_loss") or 0) or entry * (1 - self.runtime.sl_percent/100.0 if action=="LONG"
                                                             else 1 + self.runtime.sl_percent/100.0)
            tp = float(sig.get("take_profit") or 0) or entry * (1 + self.runtime.tp_percent/100.0 if action=="LONG"
                                                               else 1 - self.runtime.tp_percent/100.0)
            sl = rules.round_price(sl); tp = rules.round_price(tp)
            # Safety: SL on correct side
            if action == "LONG" and sl >= entry: sl = rules.round_price(entry * 0.995)
            if action == "SHORT" and sl <= entry: sl = rules.round_price(entry * 1.005)

            remaining = max(1, self.runtime.max_open_positions - self.open_trades_count())
            qty = size_from_risk(equity, self.settings.risk_per_trade_pct,
                                 entry, sl, rules, self.runtime.scale_factor,
                                 avail, self.runtime.leverage, remaining)
            if qty <= 0:
                self.errors.log("EXEC", "qty", symbol, RuntimeError("qty=0"))
                return None

            is_shadow = (self.runtime.mode != "LIVE") or (GLOBAL["live_owner"] not in (None, self.pid))
            trade_uid = f"p{self.pid}-{symbol}-{int(time.time()*1000)}"

            if is_shadow:
                console_log("INFO", f"[P{self.pid}][SHADOW] {action} {symbol} qty={qty} entry={entry} sl={sl} tp={tp}")
                self.record_trade_open(trade_uid, symbol, side, entry, qty, sl, tp, True)
                self._notify_open(trade_uid, symbol, side, entry, qty, sl, tp, shadow=True)
                return trade_uid

            # LIVE
            try:
                self.bybit.set_leverage(symbol, self.runtime.leverage)
            except Exception: pass
            try:
                self.bybit.create_order(symbol, side, str(qty), "Market")
            except Exception as e:
                self.errors.log("EXEC", "create_order", symbol, e)
                self.tg.send(f"⚠️ P{self.pid} entry failed {symbol}: {e}")
                return None
            try:
                self.bybit.set_trading_stop(symbol, stop_loss=str(sl), take_profit=str(tp))
            except Exception as e:
                self.errors.log("EXEC", "set_sl_tp", symbol, e, trade_uid)
                self.tg.send(f"🚨 P{self.pid} SL/TP failed {symbol}: {e}")

            self.record_trade_open(trade_uid, symbol, side, entry, qty, sl, tp, False)
            self._notify_open(trade_uid, symbol, side, entry, qty, sl, tp, shadow=False)
            return trade_uid
        except Exception as e:
            self.errors.log("EXEC", "execute_signal_outer", symbol, e)
            return None

    def _equity_and_avail(self):
        try:
            wb = self.bybit.wallet_balance()
            lst = wb.get("list", [])
            if not lst: return 0.0, 0.0
            acct = lst[0]
            equity = float(acct.get("totalEquity") or 0)
            avail = float(acct.get("totalAvailableBalance") or 0)
            return equity, avail
        except Exception as e:
            self.errors.log("EXEC", "wallet", "", e)
            return 0.0, 0.0

    def _notify_open(self, trade_uid, symbol, side, entry, qty, sl, tp, shadow):
        tag = "🧪 SHADOW" if shadow else "🔴 LIVE"
        txt = (f"{tag} <b>OPEN</b> · P{self.pid}\n"
               f"<code>{symbol}</code> {side}\n"
               f"entry=<code>{entry}</code> qty=<code>{qty}</code>\n"
               f"SL=<code>{sl}</code> TP=<code>{tp}</code>\n"
               f"uid=<code>{trade_uid}</code>")
        if GLOBAL["active_profile"] == self.pid:
            self.tg.send(txt)

    def check_daily_limits(self) -> bool:
        now = int(time.time()*1000)
        day_ms = 86400_000
        if now - self.runtime.daily_anchor_ms > day_ms:
            self.runtime.daily_anchor_ms = now
            self.runtime.daily_pnl = 0.0
        # (Real PnL aggregation deferred to closed-pnl endpoint)
        if abs(self.runtime.daily_pnl) / 100.0 > self.settings.max_daily_loss_pct and self.runtime.daily_pnl < 0:
            return False
        return True

# ════════════════════════════════════════════════════════════════
# SECTION 10: ORCHESTRATOR
# ════════════════════════════════════════════════════════════════

class Orchestrator:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.system_errors = TechErrorLogger(
            os.path.join(settings.data_root, "system_errors.log"), 0)
        self.bybit = BybitClient(settings.bybit_api_key, settings.bybit_api_secret,
                                 settings.testnet, self.system_errors)
        self.rules = SymbolRulesCache(self.bybit)
        self.tg = TelegramObserver(settings.tg_token, settings.tg_chat_id,
                                   settings.allowed_users, self.system_errors)
        self.profiles: Dict[int, Profile] = {}
        self._running = False
        self._register_commands()

    # ─── Command registration ─────────────────────────────────────
    def _register_commands(self):
        r = self.tg.register
        r("start", self.cmd_start)
        r("help", self.cmd_start)
        r("p", self.cmd_p)
        r("status", self.cmd_status)
        r("overview", self.cmd_overview)
        r("strategy", self.cmd_strategy)
        r("strategy_reload", self.cmd_strategy_reload)
        r("strategy_rollback", self.cmd_strategy_rollback)
        r("strategy_validate", self.cmd_strategy_validate)
        r("strategy_get", self.cmd_strategy_get)
        r("strategy_backups", self.cmd_strategy_backups)
        r("shadow_on", self.cmd_shadow_on)
        r("shadow_off", self.cmd_shadow_off)
        r("confirm_live", self.cmd_confirm_live)
        r("trades", self.cmd_trades)
        r("errors", self.cmd_errors)
        r("ip", self.cmd_ip)
        r("restart", self.cmd_restart)
        r("confirm_restart", self.cmd_confirm_restart)
        r("cancel_restart", self.cmd_cancel_restart)
        r("why", self.cmd_why)
        r("log", self.cmd_log)
        self.tg.doc_handler = self.cmd_document

    def _active(self) -> Profile:
        return self.profiles[GLOBAL["active_profile"]]

    def cmd_start(self, chat_id, ctx):
        self.tg.reply(chat_id, (
            "🤖 <b>Bybit Bot</b>\n"
            "/p &lt;1-4&gt; — switch profile\n"
            "/overview — all profiles\n/status — active profile\n"
            "/strategy — strategy info\n/strategy_reload | /strategy_rollback\n"
            "/trades [N] | /errors | /ip\n"
            "/shadow_on | /shadow_off + /confirm_live\n"
            "/restart + /confirm_restart\n"
            "Upload a .py file to replace strategy for a chosen profile."))

    def cmd_p(self, chat_id, ctx):
        args = ctx.get("args") or []
        if not args:
            return self.tg.reply(chat_id, f"Active profile: P{GLOBAL['active_profile']}")
        try:
            pid = int(args[0])
            if pid not in self.profiles:
                return self.tg.reply(chat_id, "Invalid profile (1-4)")
            GLOBAL["active_profile"] = pid
            self.tg.reply(chat_id, f"✅ Active profile → P{pid}")
        except ValueError:
            self.tg.reply(chat_id, "Usage: /p 1-4")

    def cmd_status(self, chat_id, ctx):
        p = self._active()
        st = p.strategy
        txt = (f"<b>P{p.pid} — {p.runtime.mode}</b>\n"
               f"strategy: {st.name} v{st.version} ({st.loaded_from})\n"
               f"tf={p.runtime.timeframe} lev={p.runtime.leverage} "
               f"scale={p.runtime.scale_factor} max_pos={p.runtime.max_open_positions}\n"
               f"sl%={p.runtime.sl_percent} tp%={p.runtime.tp_percent}\n"
               f"open trades: {p.open_trades_count()}\n"
               f"live_owner: {GLOBAL['live_owner']}")
        self.tg.reply(chat_id, txt)

    def cmd_overview(self, chat_id, ctx):
        lines = ["<b>Overview</b>"]
        for pid, p in self.profiles.items():
            marker = "⭐" if pid == GLOBAL["active_profile"] else "  "
            lines.append(f"{marker} P{pid}: {p.runtime.mode} · open={p.open_trades_count()} "
                         f"· strat={p.strategy.name}")
        self.tg.reply(chat_id, "\n".join(lines))

    def cmd_strategy(self, chat_id, ctx):
        p = self._active()
        s = p.strategy
        self.tg.reply(chat_id, (f"<b>P{p.pid} strategy</b>\n"
                                f"name: {s.name}\nversion: {s.version}\n"
                                f"sha: {s.file_sha}\nloaded_from: {s.loaded_from}\n"
                                f"paths: {s.primary_path}"))

    def cmd_strategy_reload(self, chat_id, ctx):
        p = self._active()
        ok = p.strategy.load()
        self.tg.reply(chat_id, f"Reload: {'✅ OK' if ok else '❌ failed (fell back to LKG/none)'}")

    def cmd_strategy_rollback(self, chat_id, ctx):
        p = self._active()
        res = p.strategy.rollback()
        self.tg.reply(chat_id, f"Rollback: {res['msg']}")

    def cmd_strategy_validate(self, chat_id, ctx):
        p = self._active()
        try:
            p.strategy._dummy_test(p.strategy._mod) if p.strategy._mod else None
            self.tg.reply(chat_id, "✅ Strategy passes validation")
        except Exception as e:
            self.tg.reply(chat_id, f"❌ {e}")

    def cmd_strategy_get(self, chat_id, ctx):
        p = self._active()
        try:
            with open(p.strategy.primary_path, "rb") as f: data = f.read()
            # Send as document
            url = self.tg._url("sendDocument")
            files = {"document": ("strategy.py", data, "text/plain")}
            data_f = {"chat_id": chat_id, "caption": f"P{p.pid} strategy.py"}
            requests.post(url, files=files, data=data_f, timeout=20)
        except Exception as e:
            self.tg.reply(chat_id, f"❌ {e}")

    def cmd_strategy_backups(self, chat_id, ctx):
        p = self._active()
        try:
            items = sorted(os.listdir(p.strategy.backups_dir), reverse=True)[:10]
            self.tg.reply(chat_id, "Backups:\n" + ("\n".join(items) or "(none)"))
        except Exception as e:
            self.tg.reply(chat_id, f"❌ {e}")

    def cmd_shadow_on(self, chat_id, ctx):
        self._active().runtime.mode = "SHADOW"
        self.tg.reply(chat_id, "🧪 SHADOW ON")

    def cmd_shadow_off(self, chat_id, ctx):
        self.tg.reply(chat_id, "⚠️ This will enable LIVE mode. Send /confirm_live within 5 minutes.")
        GLOBAL["_pending_live_ts"] = time.time()

    def cmd_confirm_live(self, chat_id, ctx):
        ts = GLOBAL.get("_pending_live_ts", 0)
        if time.time() - ts > 300:
            return self.tg.reply(chat_id, "⏱ Expired. Send /shadow_off again.")
        p = self._active()
        owner = GLOBAL["live_owner"]
        if owner is not None and owner != p.pid:
            return self.tg.reply(chat_id, f"⚠️ P{owner} already owns LIVE. Release first.")
        p.runtime.mode = "LIVE"
        GLOBAL["live_owner"] = p.pid
        self.tg.reply(chat_id, f"🔴 P{p.pid} is now LIVE owner")

    def cmd_trades(self, chat_id, ctx):
        p = self._active()
        n = 10
        if ctx.get("args"):
            try: n = min(50, int(ctx["args"][0]))
            except Exception: pass
        rows = p.state_db.execute(
            "SELECT * FROM trades ORDER BY opened_at DESC LIMIT ?", (n,)).fetchall()
        if not rows:
            return self.tg.reply(chat_id, "No trades")
        out = ["<b>Last trades</b>"]
        for r in rows:
            out.append(f"{r['opened_at']} {r['symbol']} {r['side']} "
                       f"{r['status']} pnl={r['realized_pnl']}")
        self.tg.reply(chat_id, "\n".join(out))

    def cmd_errors(self, chat_id, ctx):
        p = self._active()
        try:
            with open(p.errors.path, "r", encoding="utf-8") as f:
                lines = f.readlines()[-25:]
            self.tg.reply(chat_id, "".join(lines)[-4000:] or "(empty)")
        except FileNotFoundError:
            self.tg.reply(chat_id, "(no errors log)")
        except Exception as e:
            self.tg.reply(chat_id, f"❌ {e}")

    def cmd_ip(self, chat_id, ctx):
        self.tg.reply(chat_id, f"Public IP: <code>{self.bybit.public_ip()}</code>\n"
                              "Add to Bybit API → IP Whitelist.")

    def cmd_restart(self, chat_id, ctx):
        GLOBAL["_pending_restart_ts"] = time.time()
        self.tg.reply(chat_id, "⚠️ Send /confirm_restart within 2 minutes.")

    def cmd_confirm_restart(self, chat_id, ctx):
        if time.time() - GLOBAL.get("_pending_restart_ts", 0) > 120:
            return self.tg.reply(chat_id, "⏱ Expired.")
        self.tg.reply(chat_id, "♻️ Restarting...")
        time.sleep(1)
        os._exit(0)

    def cmd_cancel_restart(self, chat_id, ctx):
        GLOBAL["_pending_restart_ts"] = 0
        self.tg.reply(chat_id, "Restart cancelled")

    def cmd_why(self, chat_id, ctx):
        p = self._active()
        row = p.state_db.execute(
            "SELECT COUNT(*) c FROM signals WHERE signal_time_ms > ?",
            (int((time.time()-600)*1000),)).fetchone()
        self.tg.reply(chat_id, f"Signals last 10m: {row['c'] if row else 0}\n"
                              f"Open: {p.open_trades_count()} / max {p.runtime.max_open_positions}")

    def cmd_log(self, chat_id, ctx):
        self.cmd_errors(chat_id, ctx)

    def cmd_document(self, chat_id, doc: dict, user_id):
        """Handle uploaded .py file → strategy replacement flow."""
        try:
            file_id = doc["file_id"]
            name = doc.get("file_name", "")
            size = doc.get("file_size", 0)
            if not name.endswith(".py"):
                return self.tg.reply(chat_id, "Only .py files are accepted here.")
            if size > 512 * 1024:
                return self.tg.reply(chat_id, "File too large (max 512KB).")
            r = requests.get(self.tg._url("getFile"), params={"file_id": file_id}, timeout=15)
            file_path = r.json()["result"]["file_path"]
            file_url = f"https://api.telegram.org/file/bot{self.tg.token}/{file_path}"
            data = requests.get(file_url, timeout=30).content

            # For simplicity: apply to ACTIVE profile after syntax check
            p = self._active()
            res = p.strategy.replace(data)
            if res["ok"]:
                self.tg.reply(chat_id, f"✅ Strategy replaced for P{p.pid}: {res['msg']}")
            else:
                self.tg.reply(chat_id, f"❌ {res['msg']} (old strategy kept)")
        except Exception as e:
            self.tg.reply(chat_id, f"❌ {e}")

    # ─── Main loop ────────────────────────────────────────────────
    def run_loop(self):
        self._running = True
        tf_map = {"1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
                  "1h": "60", "2h": "120", "4h": "240"}
        while self._running:
            try:
                for pid, p in self.profiles.items():
                    if not p.check_daily_limits():
                        continue
                    if p.open_trades_count() >= p.runtime.max_open_positions:
                        continue
                    for symbol in self.settings.symbols:
                        try:
                            self._scan_one(p, symbol, tf_map)
                        except Exception as e:
                            p.errors.log("LOOP", "scan", symbol, e)
                time.sleep(self.settings.poll_interval)
            except KeyboardInterrupt:
                self._running = False
            except Exception as e:
                self.system_errors.log("LOOP", "run_loop", "", e)
                time.sleep(5)

    def _scan_one(self, p: Profile, symbol: str, tf_map: dict):
        if p.symbol_has_open(symbol): return
        last_ms = p.runtime.last_entry_ms.get(symbol, 0)
        if time.time()*1000 - last_ms < self.settings.cooldown_sec * 1000: return
        interval = tf_map.get(p.runtime.timeframe, "5")
        df = self.bybit.kline(symbol, interval, limit=200)
        if df.empty or len(df) < 60: return
        last_price = float(df["close"].iloc[-1])
        signal_key = f"{symbol}:{int(df['open_time'].iloc[-1])}"
        if p.runtime.last_signal_key.get(symbol) == signal_key: return

        ctx = StrategyContext(symbol=symbol, timeframe=p.runtime.timeframe,
                              now_ms=int(time.time()*1000), last_price=last_price,
                              df=df, data={}, metadata={}, config={}, signal_key=signal_key)
        out = p.strategy.call(ctx)
        action = out.get("action", "HOLD")
        p.runtime.last_signal_key[symbol] = signal_key
        if action in ("LONG", "SHORT"):
            sig = {
                "signal_id": f"p{p.pid}-{signal_key}",
                "symbol": symbol, "side": action,
                "strategy": p.strategy.name, "ts": int(time.time()*1000),
                "ref": out.get("reference_price") or last_price,
                "confidence": out.get("confidence", 0.5),
                "reason": out.get("reason", ""),
                "action": action,
                "stop_loss": out.get("stop_loss"),
                "take_profit": out.get("take_profit"),
            }
            p.record_signal(sig)
            uid = p.execute_signal(symbol, sig)
            if uid:
                p.runtime.last_entry_ms[symbol] = int(time.time()*1000)
                ind = out.get("metadata") or {"reason": sig["reason"]}
                p.vault_record_entry(uid, symbol, action, p.runtime.mode,
                                     p.strategy.name, ind, sig["reason"],
                                     sig["ref"])

    def heartbeat_loop(self):
        while True:
            try:
                time.sleep(45)
                console_log("INFO", f"💚 ENGINE ALIVE pid={os.getpid()} "
                                    f"active=P{GLOBAL['active_profile']} "
                                    f"owner={GLOBAL['live_owner']}")
            except Exception:
                pass

    def daily_report_loop(self):
        while True:
            try:
                now = datetime.now(timezone.utc)
                next_mid = now.replace(hour=0, minute=0, second=0, microsecond=0)
                wait = (next_mid.timestamp() + 86400 - now.timestamp())
                time.sleep(wait)
                lines = ["<b>📊 Daily Report</b>"]
                for pid, p in self.profiles.items():
                    lines.append(f"P{pid} [{p.runtime.mode}] strategy={p.strategy.name} "
                                 f"open={p.open_trades_count()}")
                self.tg.send("\n".join(lines))
            except Exception as e:
                self.system_errors.log("REPORT", "daily", "", e)
                time.sleep(60)

# ════════════════════════════════════════════════════════════════
# SECTION 10: BOOT SEQUENCE
# ════════════════════════════════════════════════════════════════

SETTINGS = Settings()

def bootstrap():
    # PHASE 0
    console_log("INFO", f"Python {sys.version.split()[0]} · CWD {os.getcwd()} · UTC {datetime.now(timezone.utc).isoformat()}")

    # PHASE 1
    errs = SETTINGS.validate()
    if errs:
        console_log("ERROR", f"Config errors: {errs}")
    # PHASE 2: logging (already via console_log)
    # PHASE 3: health
    start_health_server(SETTINGS.port)

    # PHASE 4: Telegram FIRST
    orch = Orchestrator(SETTINGS)
    orch.tg.start_polling()
    ip = orch.bybit.public_ip()
    orch.tg.send(f"🤖 <b>Bot starting</b>\nPublic IP: <code>{ip}</code>\n"
                 f"Mode: SHADOW (send /shadow_off + /confirm_live to go LIVE)")

    # PHASE 5: Bybit (non-fatal)
    try:
        _ = orch.bybit.ticker("BTCUSDT")
        console_log("INFO", "Bybit reachable")
    except Exception as e:
        orch.tg.send(f"⚠️ Bybit unreachable at boot: {e}. Continuing; will retry.")
        console_log("WARN", f"Bybit unreachable: {e}")

    # PHASE 6: symbol rules
    for sym in SETTINGS.symbols:
        try: orch.rules.get(sym)
        except Exception as e: console_log("WARN", f"rules {sym}: {e}")

    # PHASE 7: profiles
    for pid in (1, 2, 3, 4):
        pdir = os.path.join(SETTINGS.data_root, f"p{pid}")
        prof = Profile(pid, pdir, SETTINGS, orch.bybit, orch.rules, orch.tg)
        # Initial strategy: try primary → LKG → none
        prof.strategy.load()
        orch.profiles[pid] = prof
        console_log("INFO", f"P{pid} ready (strategy={prof.strategy.name}, from={prof.strategy.loaded_from})")

    # PHASE 8: recovery (basic - mark orphan OPEN trades)
    for p in orch.profiles.values():
        try:
            open_rows = p.state_db.execute("SELECT trade_uid FROM trades WHERE status='OPEN'").fetchall()
            if open_rows:
                console_log("INFO", f"P{p.pid} has {len(open_rows)} open trades (recovery stub)")
        except Exception: pass

    # PHASE 9: daily report thread
    threading.Thread(target=orch.daily_report_loop, daemon=True, name="daily-report").start()
    # PHASE 10: heartbeat
    threading.Thread(target=orch.heartbeat_loop, daemon=True, name="heartbeat").start()

    # PHASE 11: ready
    GLOBAL["ready"] = True
    orch.tg.send("✅ <b>ENGINE READY</b>\nActive profile: P1\nUse /overview, /status, /strategy")

    # PHASE 12: main loop
    try:
        orch.run_loop()
    except KeyboardInterrupt:
        console_log("INFO", "Shutdown requested")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ip-only", action="store_true", help="Print public IP and exit")
    args = ap.parse_args()
    if args.ip_only:
        print(BybitClient("", "", False, TechErrorLogger("/tmp/_x.log")).public_ip())
        sys.exit(0)
    bootstrap()
