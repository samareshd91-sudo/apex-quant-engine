import streamlit as st

# ==============================================================================
# 0. STREAMLIT CONFIG & STABLE RUNTIME IDENTITY
# ==============================================================================
st.set_page_config(page_title="Prime Samaresh Engine v77", page_icon="⚙️", layout="wide", initial_sidebar_state="collapsed")

import ccxt
import pandas as pd
import numpy as np
import time
import threading
import logging
import requests
import os
import gc
import uuid
import hashlib
import random
import json
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
import sqlite3
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from streamlit_autorefresh import st_autorefresh

@st.cache_resource
def get_runtime_identity():
    return str(uuid.uuid4())[:8]

@st.cache_resource
def get_telegram_session():
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
    session.mount("https://", adapter)
    return session

INSTANCE_ID = get_runtime_identity()
TELEGRAM_SESSION = get_telegram_session()

# ==============================================================================
# 1. STREAMLIT SECRETS, SQLITE LEDGER & AUTO-CLEANUP
# ==============================================================================
def get_secret(name, default=None):
    """Read from Streamlit Cloud Secrets first, then environment variables."""
    try:
        value = st.secrets.get(name, None)
        if value not in (None, ""):
            return str(value)
    except Exception:
        pass
    value = os.environ.get(name, default)
    return str(value) if value not in (None, "") else default

# Streamlit Cloud -> Settings -> Secrets
# TELEGRAM_BOT_TOKEN = "123456:ABC..."
# TELEGRAM_CHAT_ID = "123456789"
# No DATABASE_URL is required. SQLite is used automatically.
TELEGRAM_BOT_TOKEN = get_secret("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_secret("TELEGRAM_CHAT_ID")

# SQLite runtime database. /tmp is intentionally used on Streamlit Cloud so
# the ledger never consumes the repository disk and can be rebuilt on restart.
SQLITE_DB_PATH = os.environ.get("PRIME_SAMARESH_SQLITE_PATH", "/tmp/prime_samaresh_v77.db")
SQLITE_MAX_BYTES = 16 * 1024 * 1024          # cleanup starts well before storage is full
SQLITE_EMERGENCY_BYTES = 24 * 1024 * 1024    # emergency purge + VACUUM
SQLITE_MAX_ROWS = 5000
SQLITE_BUSY_TIMEOUT_MS = 5000

COINS = ['ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']
LEASE_DURATION_SEC = 180
COOLDOWN_MINUTES = 15
MAX_DELIVERY_ATTEMPTS = 2
MAX_TELEGRAM_BACKOFF_SEC = 30
OHLCV_LIMIT = 240

LEDGER_CLAIMED = "CLAIMED"
LEDGER_SENDING = "SENDING"
LEDGER_SENT = "SENT"
LEDGER_FAILED = "FAILED"
LEDGER_UNKNOWN = "UNKNOWN_DELIVERY"
LEDGER_AUDIT = "DELIVERY_AUDIT_REQUIRED"

# Frequent lightweight cleanup keeps the ephemeral Streamlit filesystem small.
LEDGER_CLEANUP_INTERVAL_SEC = 300
LEDGER_SENT_RETENTION_MINUTES = 60
LEDGER_FAILED_RETENTION_MINUTES = 180
LEDGER_AUDIT_RETENTION_MINUTES = 720

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================================================================
# 2. GLOBAL LOCKS, SQLITE CONNECTIONS & HEARTBEAT STATE
# ==============================================================================
@st.cache_resource
def get_global_primitives():
    return threading.Lock(), threading.Event(), {
        "last_reconcile": 0.0,
        "last_ledger_cleanup": 0.0,
        "last_scan_completed": 0.0,
        "last_heartbeat": time.time(),
        "scan_counter": 0,
        "data_feed_errors": 0,
        "is_leader": False,
        "worker_health": "OFFLINE 🔴"
    }

WORKER_LOCK, STOP_EVENT, SHARED_STATE = get_global_primitives()

@contextmanager
def get_db_connection():
    """Open a short-lived SQLite connection; safe for Streamlit worker threads."""
    conn = None
    try:
        conn = sqlite3.connect(
            SQLITE_DB_PATH,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        yield conn
    except Exception:
        if conn is not None:
            try: conn.rollback()
            except Exception: pass
        raise
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass

# ==============================================================================
# 3. SQLITE MIGRATION & HEALTH CHECK
# ==============================================================================
def check_db_health():
    try:
        with get_db_connection() as conn:
            conn.execute("SELECT 1;").fetchone()
            return True
    except Exception as e:
        logging.error(f"SQLite health check failed: {e}")
        return False


def initialize_database():
    try:
        with get_db_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS signals_ledger (
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT '{LEDGER_CLAIMED}',
                    claimer TEXT,
                    lease_expires TEXT,
                    takeover_count INTEGER NOT NULL DEFAULT 0,
                    claim_attempts INTEGER NOT NULL DEFAULT 0,
                    delivery_attempts INTEGER NOT NULL DEFAULT 0,
                    claim_version INTEGER NOT NULL DEFAULT 0,
                    telegram_message_id INTEGER,
                    telegram_chat_id TEXT,
                    telegram_response_audit TEXT,
                    sent_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_ledger_status_lease ON signals_ledger(status, lease_expires);",
                "CREATE INDEX IF NOT EXISTS idx_ledger_claimer ON signals_ledger(claimer);",
                "CREATE INDEX IF NOT EXISTS idx_symbol_sent ON signals_ledger(symbol, sent_at);",
                "CREATE INDEX IF NOT EXISTS idx_ledger_updated ON signals_ledger(updated_at);",
            ]
            for q in indexes:
                conn.execute(q)
            conn.commit()
            logging.info(f"SQLite ledger ready: {SQLITE_DB_PATH}")
            return True
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        logging.critical(f"SQLite migration failed: {e}")
        return False

# ==============================================================================
# 4. RECONCILIATION WORKER & AUTOMATIC PRE-FULL CLEANUP
# ==============================================================================
def reconcile_ledger(force=False):
    current_time = time.time()
    if not force and (current_time - SHARED_STATE["last_reconcile"] < 300):
        return
    try:
        with get_db_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(f"""
                UPDATE signals_ledger SET status = ?, updated_at = CURRENT_TIMESTAMP,
                last_error = ?
                WHERE status = ? AND datetime(updated_at) < datetime('now','-15 minutes')
            """, (LEDGER_AUDIT, "Reconciled: stale UNKNOWN delivery", LEDGER_UNKNOWN))
            conn.execute(f"""
                UPDATE signals_ledger SET status = ?, updated_at = CURRENT_TIMESTAMP,
                last_error = ?
                WHERE status = ? AND datetime(updated_at) < datetime('now','-5 minutes')
            """, (LEDGER_AUDIT, "Reconciled: stale SENDING state", LEDGER_SENDING))
            conn.commit()
            SHARED_STATE["last_reconcile"] = current_time
    except Exception as e:
        logging.error(f"Reconciliation error: {e}")


def _sqlite_size_bytes():
    try:
        total = os.path.getsize(SQLITE_DB_PATH)
        for suffix in ("-wal", "-shm"):
            path = SQLITE_DB_PATH + suffix
            if os.path.exists(path): total += os.path.getsize(path)
        return total
    except OSError:
        return 0


def cleanup_old_ledger_records(force=False):
    current_time = time.time()
    last_cleanup = SHARED_STATE.get("last_ledger_cleanup", 0.0)
    size_before = _sqlite_size_bytes()
    if not force and (current_time - last_cleanup < LEDGER_CLEANUP_INTERVAL_SEC) and size_before < SQLITE_MAX_BYTES:
        return
    try:
        with get_db_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            # Terminal records are disposable first. Active delivery states are preserved.
            conn.execute("DELETE FROM signals_ledger WHERE status = ? AND datetime(updated_at) < datetime('now', ?)",
                         (LEDGER_SENT, f"-{LEDGER_SENT_RETENTION_MINUTES} minutes"))
            sent_deleted = conn.total_changes
            conn.execute("DELETE FROM signals_ledger WHERE status = ? AND datetime(updated_at) < datetime('now', ?)",
                         (LEDGER_FAILED, f"-{LEDGER_FAILED_RETENTION_MINUTES} minutes"))
            failed_deleted = conn.total_changes - sent_deleted
            conn.execute("DELETE FROM signals_ledger WHERE status = ? AND datetime(updated_at) < datetime('now', ?)",
                         (LEDGER_AUDIT, f"-{LEDGER_AUDIT_RETENTION_MINUTES} minutes"))
            audit_deleted = conn.total_changes - sent_deleted - failed_deleted

            # Hard row cap: delete oldest terminal rows until comfortably below the cap.
            row_count = conn.execute("SELECT COUNT(*) FROM signals_ledger").fetchone()[0]
            if row_count > SQLITE_MAX_ROWS:
                excess = row_count - SQLITE_MAX_ROWS
                conn.execute("""
                    DELETE FROM signals_ledger
                    WHERE rowid IN (
                        SELECT rowid FROM signals_ledger
                        WHERE status IN (?, ?, ?)
                        ORDER BY datetime(updated_at) ASC
                        LIMIT ?
                    )
                """, (LEDGER_SENT, LEDGER_FAILED, LEDGER_AUDIT, excess))

            conn.commit()

        size_after = _sqlite_size_bytes()
        if size_after >= SQLITE_MAX_BYTES:
            # Compact before storage gets anywhere near full.
            with get_db_connection() as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                conn.execute("VACUUM;")
        if _sqlite_size_bytes() >= SQLITE_EMERGENCY_BYTES:
            # Last-resort automatic purge of oldest terminal history.
            with get_db_connection() as conn:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute("""
                    DELETE FROM signals_ledger WHERE rowid IN (
                        SELECT rowid FROM signals_ledger
                        WHERE status IN (?, ?, ?)
                        ORDER BY datetime(updated_at) ASC LIMIT 1000
                    )
                """, (LEDGER_SENT, LEDGER_FAILED, LEDGER_AUDIT))
                conn.commit()
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                conn.execute("VACUUM;")

        SHARED_STATE["last_ledger_cleanup"] = current_time
        total_deleted = sent_deleted + failed_deleted + audit_deleted
        if total_deleted:
            logging.info(f"SQLite auto-cleanup: SENT={sent_deleted}, FAILED={failed_deleted}, AUDIT={audit_deleted}")
    except Exception as e:
        logging.error(f"SQLite auto-cleanup failed safely: {e}")


def get_pending_audit_count():
    try:
        with get_db_connection() as conn:
            return conn.execute("SELECT COUNT(*) FROM signals_ledger WHERE status = ?", (LEDGER_AUDIT,)).fetchone()[0]
    except Exception:
        return 0

# ==============================================================================
# 5. ATOMIC CLAIMING & LOCAL FENCING
# ==============================================================================
def acquire_distributed_lease(signal_id, symbol, direction):
    now = datetime.now(timezone.utc)
    lease_expires = now + timedelta(seconds=LEASE_DURATION_SEC)
    now_s = now.isoformat()
    lease_s = lease_expires.isoformat()
    try:
        with get_db_connection() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            # One process/thread owns the scanner. SQLite transaction serialization prevents duplicate claims.
            blocked = conn.execute("""
                SELECT 1 FROM signals_ledger
                WHERE symbol = ? AND (
                    (status = ? AND sent_at IS NOT NULL AND datetime(sent_at) > datetime('now', ?))
                    OR (status IN (?, ?, ?) AND datetime(updated_at) > datetime('now','-10 minutes'))
                ) LIMIT 1
            """, (symbol, LEDGER_SENT, f"-{COOLDOWN_MINUTES} minutes",
                   LEDGER_CLAIMED, LEDGER_SENDING, LEDGER_UNKNOWN)).fetchone()
            if blocked:
                conn.rollback()
                return None

            row = conn.execute("SELECT * FROM signals_ledger WHERE signal_id = ?", (signal_id,)).fetchone()
            if row is None:
                conn.execute("""
                    INSERT INTO signals_ledger
                    (signal_id,symbol,direction,status,claimer,lease_expires,claim_attempts,claim_version,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                """, (signal_id, symbol, direction, LEDGER_CLAIMED, INSTANCE_ID, lease_s, 1, 1))
                claim_version = 1
            elif (row["status"] == LEDGER_FAILED and row["delivery_attempts"] < MAX_DELIVERY_ATTEMPTS) or \
                 (row["status"] == LEDGER_CLAIMED and row["lease_expires"] and row["lease_expires"] < now_s):
                claim_version = int(row["claim_version"] or 0) + 1
                takeover = int(row["takeover_count"] or 0) + (1 if row["claimer"] != INSTANCE_ID else 0)
                conn.execute("""
                    UPDATE signals_ledger SET status=?, claimer=?, lease_expires=?, takeover_count=?,
                    claim_attempts=claim_attempts+1, claim_version=?, last_error=NULL, updated_at=CURRENT_TIMESTAMP
                    WHERE signal_id=?
                """, (LEDGER_CLAIMED, INSTANCE_ID, lease_s, takeover, claim_version, signal_id))
            else:
                conn.rollback()
                return None
            conn.commit()
            return {"signal_id": signal_id, "claim_version": claim_version, "lease_expires": lease_s}
    except Exception as e:
        logging.error(f"Atomic SQLite claim error: {e}")
        try: conn.rollback()
        except Exception: pass
        return None


def transition_to_sending(signal_id, claim_version):
    now = datetime.now(timezone.utc).isoformat()
    try:
        with get_db_connection() as conn:
            cur = conn.execute(f"""
                UPDATE signals_ledger SET status=?, delivery_attempts=delivery_attempts+1, updated_at=CURRENT_TIMESTAMP
                WHERE signal_id=? AND claimer=? AND claim_version=? AND status=? AND lease_expires>?
            """, (LEDGER_SENDING, signal_id, INSTANCE_ID, claim_version, LEDGER_CLAIMED, now))
            return cur.rowcount == 1
    except Exception:
        return False


def verify_preflight_fencing(signal_id, claim_version):
    try:
        with get_db_connection() as conn:
            return conn.execute("""
                SELECT 1 FROM signals_ledger
                WHERE signal_id=? AND claimer=? AND claim_version=? AND status=? AND lease_expires>?
            """, (signal_id, INSTANCE_ID, claim_version, LEDGER_SENDING,
                   datetime.now(timezone.utc).isoformat())).fetchone() is not None
    except Exception:
        return False


def commit_signal_state(signal_id, claim_version, target_status, message_id=None, chat_id=None, response_audit=None, error_message=None):
    try:
        with get_db_connection() as conn:
            audit_data = json.dumps(response_audit) if response_audit else None
            if target_status == LEDGER_SENT:
                cur = conn.execute("""
                    UPDATE signals_ledger SET status=?, telegram_message_id=?, telegram_chat_id=?,
                    telegram_response_audit=?, sent_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
                    WHERE signal_id=? AND claimer=? AND claim_version=? AND status=?
                """, (LEDGER_SENT, message_id, str(chat_id), audit_data, signal_id, INSTANCE_ID, claim_version, LEDGER_SENDING))
            else:
                cur = conn.execute("""
                    UPDATE signals_ledger SET status=?, last_error=?, telegram_response_audit=?, updated_at=CURRENT_TIMESTAMP
                    WHERE signal_id=? AND claimer=? AND claim_version=? AND status=?
                """, (target_status, str(error_message)[:2000] if error_message else None,
                      audit_data, signal_id, INSTANCE_ID, claim_version, LEDGER_SENDING))
            return cur.rowcount == 1
    except Exception:
        return False

# ==============================================================================
# 6. SMC LIFECYCLE ENGINE
# ==============================================================================
def fetch_ohlcv(exchange, symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=OHLCV_LIMIT)
        if not ohlcv:
            SHARED_STATE["data_feed_errors"] += 1
            return None
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        exchange_time_ms = exchange.milliseconds()
        tf_minutes = 5 
        if timeframe.endswith('m'): tf_minutes = int(timeframe.replace('m', ''))
        elif timeframe.endswith('h'): tf_minutes = int(timeframe.replace('h', '')) * 60
        last_open_ms = int(df['timestamp'].iloc[-1].timestamp() * 1000)
        tf_ms = tf_minutes * 60 * 1000
        if last_open_ms + tf_ms > exchange_time_ms:
            df = df.iloc[:-1].copy()
        return df
    except Exception as e:
        SHARED_STATE["data_feed_errors"] += 1
        return None

def calculate_smc_features(df):
    df['ema_50'] = df['close'].ewm(span=50, adjust=False, min_periods=50).mean()
    df['ema_200'] = df['close'].ewm(span=200, adjust=False, min_periods=200).mean()
    high = df['high'].to_numpy(copy=False)
    low = df['low'].to_numpy(copy=False)
    close = df['close'].to_numpy(copy=False)
    prev_close = np.empty_like(close)
    prev_close[0] = np.nan
    prev_close[1:] = close[:-1]

    tr = np.maximum.reduce([high - low, np.abs(high - prev_close), np.abs(low - prev_close)])
    df['atr'] = pd.Series(tr, index=df.index).ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    atr_min, atr_max = df['atr'].rolling(window=100, min_periods=50).min(), df['atr'].rolling(window=100, min_periods=50).max()
    df['atr_position'] = ((df['atr'] - atr_min) / (atr_max - atr_min).replace(0, np.nan)).fillna(0.5)

    df['is_swing_high'] = (df['high'].shift(2) > df['high'].shift(1)) & (df['high'].shift(2) > df['high']) & (df['high'].shift(2) > df['high'].shift(3)) & (df['high'].shift(2) > df['high'].shift(4))
    df['is_swing_low'] = (df['low'].shift(2) < df['low'].shift(1)) & (df['low'].shift(2) < df['low']) & (df['low'].shift(2) < df['low'].shift(3)) & (df['low'].shift(2) < df['low'].shift(4))

    n = len(df)
    bos_bull, choch_bull, bos_bear, choch_bear = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool), np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    fvg_tapped_bull, fvg_tapped_bear = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    liq_sweep_bull, liq_sweep_bear = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    
    active_bull_fvgs, active_bear_fvgs = [], []
    current_trend = 0 
    last_swing_high, last_swing_low = np.nan, np.nan
    
    closes, opens, highs, lows = df['close'].values, df['open'].values, df['high'].values, df['low'].values
    is_sh, is_sl = df['is_swing_high'].values, df['is_swing_low'].values
    atrs = df['atr'].values
    
    for i in range(2, n):
        if is_sh[i]: last_swing_high = highs[i-2]
        if is_sl[i]: last_swing_low = lows[i-2]
            
        c, o, h, l = closes[i], opens[i], highs[i], lows[i]
        snap_sh, snap_sl = last_swing_high, last_swing_low

        if not np.isnan(snap_sl) and l < snap_sl and c > snap_sl: liq_sweep_bull[i] = True
        if not np.isnan(snap_sh) and h > snap_sh and c < snap_sh: liq_sweep_bear[i] = True

        if not np.isnan(snap_sh) and c > snap_sh:
            if current_trend <= 0: choch_bull[i] = True
            else: bos_bull[i] = True
            current_trend = 1
            last_swing_high = np.nan 
        elif not np.isnan(snap_sl) and c < snap_sl:
            if current_trend >= 0: choch_bear[i] = True
            else: bos_bear[i] = True
            current_trend = -1
            last_swing_low = np.nan 
            
        valid_bull_fvgs = []
        for fvg in active_bull_fvgs:
            is_tapped = (l <= fvg['top']) and (h >= fvg['bot'])
            is_invalidated = c < fvg['bot']
            if is_tapped:
                if fvg['taps'] == 0: fvg_tapped_bull[i] = True 
                fvg['taps'] += 1
            if not is_invalidated: valid_bull_fvgs.append(fvg)
        active_bull_fvgs = valid_bull_fvgs[-10:]
        
        valid_bear_fvgs = []
        for fvg in active_bear_fvgs:
            is_tapped = (h >= fvg['bot']) and (l <= fvg['top'])
            is_invalidated = c > fvg['top']
            if is_tapped:
                if fvg['taps'] == 0: fvg_tapped_bear[i] = True
                fvg['taps'] += 1
            if not is_invalidated: valid_bear_fvgs.append(fvg)
        active_bear_fvgs = valid_bear_fvgs[-10:]

        fvg_gap_bull = l - highs[i-2]
        if fvg_gap_bull >= (0.15 * atrs[i]) and c > o: 
            active_bull_fvgs.append({'top': l, 'bot': highs[i-2], 'taps': 0})
            
        fvg_gap_bear = lows[i-2] - h
        if fvg_gap_bear >= (0.15 * atrs[i]) and c < o: 
            active_bear_fvgs.append({'top': lows[i-2], 'bot': h, 'taps': 0})

    df['bos_bull'], df['choch_bull'] = bos_bull, choch_bull
    df['bos_bear'], df['choch_bear'] = bos_bear, choch_bear
    df['fvg_tapped_bull'], df['fvg_tapped_bear'] = fvg_tapped_bull, fvg_tapped_bear
    df['liq_sweep_bull'], df['liq_sweep_bear'] = liq_sweep_bull, liq_sweep_bear
    df['candle_range'] = df['high'] - df['low']
    df['body_size'] = abs(df['close'] - df['open'])
    
    avg_body = df['body_size'].shift(1).rolling(20).mean()
    avg_vol = df['volume'].shift(1).rolling(20).mean()
    
    df['is_displacement'] = (df['body_size'] > df['candle_range'] * 0.6) & (df['body_size'] > avg_body) & (df['volume'] > avg_vol)
    df['displacement_bull'] = df['is_displacement'] & (df['close'] > df['open'])
    df['displacement_bear'] = df['is_displacement'] & (df['close'] < df['open'])
    df['volume_confirmation_bull'] = (df['close'] > df['open']) & (df['volume'] > avg_vol * 1.5)
    df['volume_confirmation_bear'] = (df['close'] < df['open']) & (df['volume'] > avg_vol * 1.5)

    return df

def analyze_btc_regime(exchange):
    btc_1h = fetch_ohlcv(exchange, 'BTC/USDT', '1h')
    if btc_1h is None or btc_1h.empty or len(btc_1h) < 210: 
        return "NEUTRAL"
    close = btc_1h['close']
    btc_1h['ema_50'] = close.ewm(span=50, adjust=False, min_periods=50).mean()
    btc_1h['ema_200'] = close.ewm(span=200, adjust=False, min_periods=200).mean()
    
    avg_vol = btc_1h['volume'].shift(1).rolling(20, min_periods=20).mean()
    volume_confirmation_bull = ((btc_1h['close'] > btc_1h['open']) & (btc_1h['volume'] > avg_vol * 1.5))
    volume_confirmation_bear = ((btc_1h['close'] < btc_1h['open']) & (btc_1h['volume'] > avg_vol * 1.5))
    
    last_10 = btc_1h.iloc[-10:]
    bull_count = ((last_10['close'] > last_10['ema_50']) & (last_10['ema_50'] > last_10['ema_200'])).sum()
    bear_count = ((last_10['close'] < last_10['ema_50']) & (last_10['ema_50'] < last_10['ema_200'])).sum()
    last_bull = bool(volume_confirmation_bull.iloc[-1])
    last_bear = bool(volume_confirmation_bear.iloc[-1])
    del btc_1h
    
    if bull_count >= 7: return "STRONG_BULL" if last_bull else "BULL"
    if bear_count >= 7: return "STRONG_BEAR" if last_bear else "BEAR"
    return "NEUTRAL"

# ==============================================================================
# 7. SCORING MODEL
# ==============================================================================
def evaluate_engine_score(df_1h, df_15m, df_5m, direction, btc_regime):
    if len(df_1h) < 210 or len(df_15m) < 210 or len(df_5m) < 210: return 0, False
    c1h, c15 = df_1h.iloc[-1], df_15m.iloc[-1]
    window_5m = df_5m.iloc[-2:] 
    current_5m = df_5m.iloc[-1]
    
    cat_htf, cat_structure, cat_liq, cat_confirmation, cat_regime = 0, 0, 0, 0, 0
    
    if direction == "BULL":
        if c1h['close'] > c1h['ema_50']: cat_htf += 15
        if c1h['ema_50'] > c1h['ema_200']: cat_htf += 15
        bull_structure = (c15['bos_bull'] or c15['choch_bull'] or window_5m['bos_bull'].any() or window_5m['choch_bull'].any())
        if bull_structure: cat_structure += 20
        if c15['liq_sweep_bull'] or window_5m['liq_sweep_bull'].any(): cat_liq += 15
        if current_5m['displacement_bull']: cat_confirmation += 10
        if current_5m['fvg_tapped_bull']: cat_confirmation += 10
        if current_5m['volume_confirmation_bull']: cat_confirmation += 5
        if btc_regime in ["BULL", "STRONG_BULL"]: cat_regime += 10
            
    elif direction == "BEAR":
        if c1h['close'] < c1h['ema_50']: cat_htf += 15
        if c1h['ema_50'] < c1h['ema_200']: cat_htf += 15
        bear_structure = (c15['bos_bear'] or c15['choch_bear'] or window_5m['bos_bear'].any() or window_5m['choch_bear'].any())
        if bear_structure: cat_structure += 20
        if c15['liq_sweep_bear'] or window_5m['liq_sweep_bear'].any(): cat_liq += 15
        if current_5m['displacement_bear']: cat_confirmation += 10
        if current_5m['fvg_tapped_bear']: cat_confirmation += 10
        if current_5m['volume_confirmation_bear']: cat_confirmation += 5
        if btc_regime in ["BEAR", "STRONG_BEAR"]: cat_regime += 10
        
    total_score = cat_htf + cat_structure + cat_liq + cat_confirmation + cat_regime
    structural_valid = (cat_htf >= 15) and (cat_structure >= 20) and (cat_confirmation >= 10) and (cat_regime > 0) and (total_score >= 75)
    return total_score, structural_valid

# ==============================================================================
# 8. TELEGRAM PIPELINE 
# ==============================================================================
def distributed_signal_pipeline(symbol, direction, score, btc_regime, df_5m, atr_pos, display_icon):
    closed_ts_ms = int((df_5m['timestamp'].iloc[-1] + pd.Timedelta(minutes=5)).timestamp() * 1000)
    sig_string = f"{symbol}|{closed_ts_ms}|{direction}"
    signal_id = hashlib.sha256(sig_string.encode()).hexdigest() 
    closed_timestamp_str = (df_5m['timestamp'].iloc[-1] + pd.Timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')

    claim = acquire_distributed_lease(signal_id, symbol, direction)
    if not claim: return False
    claim_version = claim["claim_version"]
    
    if not transition_to_sending(signal_id, claim_version): return False

    msg = (f"⚡ <b>Prime Samaresh Engine v77</b>\n"
           f"🪙 <b>Pair:</b> {symbol}\n"
           f"📈 <b>Action:</b> {direction} {display_icon}\n"
           f"🔥 <b>Conviction Score:</b> {score}/100\n"
           f"⚖️ <b>BTC Regime:</b> {btc_regime}\n"
           f"⏱️ <b>Candle Close:</b> {closed_timestamp_str} UTC\n"
           f"📊 <b>ATR Position:</b> {atr_pos:.2f}\n"
           f"🆔 <b>Signal ID:</b> <code>{signal_id[:16]}...</code>")
    
    def attempt_telegram_delivery():
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        return TELEGRAM_SESSION.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
    
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            commit_signal_state(signal_id, claim_version, LEDGER_FAILED, error_message="Fatal: Telegram credentials missing in Streamlit Secrets (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
            return False
            
        if not verify_preflight_fencing(signal_id, claim_version):
            logging.warning(f"Aborting Telegram dispatch: Fencing validation failed or lease expired for {signal_id[:16]}")
            return False

        response = attempt_telegram_delivery()
        
        if response.status_code == 429:
            try: retry_data = response.json()
            except: retry_data = {}
            retry_after = retry_data.get("parameters", {}).get("retry_after", 5.0)
            if retry_after > MAX_TELEGRAM_BACKOFF_SEC:
                commit_signal_state(signal_id, claim_version, LEDGER_FAILED, response_audit=retry_data, error_message=f"Rate Limited: Retry-After ({retry_after}s) exceeds MAX_BACKOFF ({MAX_TELEGRAM_BACKOFF_SEC}s)")
                return False
            time.sleep(retry_after)
            
            if not verify_preflight_fencing(signal_id, claim_version): return False
            response = attempt_telegram_delivery()
            
        try: data = response.json() 
        except: data = {"raw_text": response.text[:500]}
        
        if response.ok and data.get("ok") is True:
            msg_id = data.get("result", {}).get("message_id")
            committed = False
            for _ in range(3):
                if commit_signal_state(signal_id, claim_version, LEDGER_SENT, message_id=msg_id, chat_id=TELEGRAM_CHAT_ID, response_audit=data):
                    committed = True
                    break
                time.sleep(1.5)
            return committed
        else:
            status_code = response.status_code
            error_msg = f"HTTP {status_code}: {response.text[:200]}"
            if status_code in [400, 401, 403, 404]:
                commit_signal_state(signal_id, claim_version, LEDGER_FAILED, response_audit=data, error_message=f"Permanent Failure: {error_msg}")
            elif status_code == 429:
                commit_signal_state(signal_id, claim_version, LEDGER_FAILED, response_audit=data, error_message=f"Rate Limited (Double hit): {error_msg}")
            else:
                commit_signal_state(signal_id, claim_version, LEDGER_UNKNOWN, response_audit=data, error_message=f"Server Error (Ambiguous): {error_msg}")
            return False

    except requests.exceptions.Timeout:
        commit_signal_state(signal_id, claim_version, LEDGER_UNKNOWN, error_message="Network Timeout during Delivery (Ambiguous)")
        return False
    except requests.exceptions.ConnectionError:
        commit_signal_state(signal_id, claim_version, LEDGER_UNKNOWN, error_message="Connection Reset during Delivery (Ambiguous)")
        return False
    except Exception as e:
        commit_signal_state(signal_id, claim_version, LEDGER_UNKNOWN, error_message=str(e))
        return False

# ==============================================================================
# 9. SCANNER CYCLE
# ==============================================================================
def run_scan_cycle(exchange):
    SHARED_STATE["data_feed_errors"] = 0
    btc_regime = analyze_btc_regime(exchange)
    reconcile_ledger()
    cleanup_old_ledger_records()

    for symbol in COINS:
        df_1h = fetch_ohlcv(exchange, symbol, '1h')
        df_15m = fetch_ohlcv(exchange, symbol, '15m')
        df_5m = fetch_ohlcv(exchange, symbol, '5m')
        
        if any(df is None or df.empty for df in [df_1h, df_15m, df_5m]): 
            del df_1h, df_15m, df_5m
            continue
        
        df_1h, df_15m, df_5m = calculate_smc_features(df_1h), calculate_smc_features(df_15m), calculate_smc_features(df_5m)
        bull_score, bull_valid = evaluate_engine_score(df_1h, df_15m, df_5m, "BULL", btc_regime)
        bear_score, bear_valid = evaluate_engine_score(df_1h, df_15m, df_5m, "BEAR", btc_regime)
        
        direction, score, display_icon = None, 0, ""
        
        if bull_valid and bear_valid:
            if bull_score >= bear_score + 10: direction, score, display_icon = "BULL", bull_score, "🟢"
            elif bear_score >= bull_score + 10: direction, score, display_icon = "BEAR", bear_score, "🔴"
            else: direction = None 
        elif bull_valid: direction, score, display_icon = "BULL", bull_score, "🟢"
        elif bear_valid: direction, score, display_icon = "BEAR", bear_score, "🔴"
            
        if direction:
            atr_pos = df_5m['atr_position'].iloc[-1]
            distributed_signal_pipeline(symbol, direction, score, btc_regime, df_5m, atr_pos, display_icon)
        
        del df_1h, df_15m, df_5m
        
    SHARED_STATE["last_scan_completed"] = time.time()
    SHARED_STATE["last_heartbeat"] = time.time() 
    SHARED_STATE["scan_counter"] += 1
    if SHARED_STATE["scan_counter"] % 6 == 0: gc.collect()

# ==============================================================================
# 10. WORKER LIFECYCLE - STREAMLIT SINGLE-WORKER MODE
# ==============================================================================
def get_next_scan_time():
    now = datetime.now(timezone.utc)
    remainder = 5 - (now.minute % 5)
    next_boundary = now + timedelta(minutes=remainder)
    return next_boundary.replace(second=0, microsecond=0).timestamp()


def scanner_worker_loop():
    logging.info(f"Prime Samaresh Engine v77 SQLite Worker Started (ID: {INSTANCE_ID})")
    exchange = None

    while not STOP_EVENT.is_set():
        try:
            SHARED_STATE["last_heartbeat"] = time.time()

            # Keep retrying instead of exiting. This prevents Streamlit reruns from
            # creating a new worker whenever a transient SQLite lock/setup error occurs.
            if not initialize_database():
                SHARED_STATE["is_leader"] = True
                SHARED_STATE["worker_health"] = "SQLITE SETUP RETRY 🔴"
                STOP_EVENT.wait(5)
                continue

            if not check_db_health():
                SHARED_STATE["is_leader"] = True
                SHARED_STATE["worker_health"] = "SQLITE UNREACHABLE 🔴"
                STOP_EVENT.wait(5)
                continue

            reconcile_ledger(force=True)
            cleanup_old_ledger_records(force=True)
            SHARED_STATE["is_leader"] = True
            SHARED_STATE["worker_health"] = "HEALTHY 🟢"

            if exchange is None:
                exchange = ccxt.kucoin({'enableRateLimit': True, 'timeout': 10000})

            # First scan immediately after startup; later scans follow 5-minute boundaries.
            if SHARED_STATE["scan_counter"] == 0:
                run_scan_cycle(exchange)
            else:
                next_scan = get_next_scan_time()
                sleep_duration = max(0.0, next_scan - time.time()) + random.uniform(2.0, 7.0)
                if STOP_EVENT.wait(sleep_duration):
                    break
                if not check_db_health():
                    continue
                run_scan_cycle(exchange)

            if SHARED_STATE["data_feed_errors"] > 0:
                SHARED_STATE["worker_health"] = "DATA FEED DOWN 🟡"
            else:
                SHARED_STATE["worker_health"] = "HEALTHY 🟢"

        except Exception as e:
            logging.exception(f"Scanner worker error: {e}")
            if exchange:
                try:
                    exchange.close()
                except Exception:
                    pass
                exchange = None
            SHARED_STATE["is_leader"] = True
            SHARED_STATE["worker_health"] = "CRASHED - RETRYING 🔴"
            STOP_EVENT.wait(5)

    if exchange:
        try:
            exchange.close()
        except Exception:
            pass


@st.cache_resource
def start_background_scanner():
    """Create exactly one scanner thread per Streamlit server process."""
    worker = threading.Thread(
        target=scanner_worker_loop,
        name="PrimeSamareshScanner",
        daemon=True,
    )
    worker.start()
    return worker

# ==============================================================================
# 11. STREAMLIT UI
# ==============================================================================
st.markdown("## Prime Samaresh Engine v77")
st.markdown("**Streamlit Cloud Edition • SQLite Auto-Cleanup • KuCoin Scanner • Telegram Delivery**")

# Initialize SQLite before rendering the dashboard so the UI never reports a
# false database outage while the worker is still starting.
initialize_database()
worker_thread = start_background_scanner()
db_healthy = check_db_health()
pending_audits = get_pending_audit_count()

worker_health = SHARED_STATE["worker_health"]
is_leader = SHARED_STATE["is_leader"]
last_scan = SHARED_STATE["last_scan_completed"]

offline_states = ["OFFLINE 🔴", "CRASHED 🔴", "SQLITE UNREACHABLE 🔴", "SQLITE SETUP FAILED 🔴"]

if not any(t.name == "PrimeSamareshScanner" and t.is_alive() for t in threading.enumerate()):
    worker_health = "OFFLINE 🔴"
elif last_scan > 0 and time.time() - last_scan > 600:
    worker_health = "DEGRADED 🟡"

role_display = "ACTIVE WORKER 👑" if is_leader else "STANDBY 🛡️"
last_scan_display = datetime.fromtimestamp(last_scan, timezone.utc).strftime('%H:%M:%S UTC') if last_scan > 0 else "Pending..."

st.metric("Engine Status", "Offline 🔴" if worker_health in offline_states else "Online 🟢")
col1, col2, col3 = st.columns(3)
col1.metric("Worker Health", worker_health)
col2.metric("Current Role", role_display)
col3.metric("Last Completed Scan", last_scan_display)

st.write("---")
st.metric("SQLite Connectivity", "Connected 🟢" if db_healthy else "Unreachable 🔴 (Scan Suspended)")
telegram_ready = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
st.metric("Telegram Delivery", "Configured 🟢" if telegram_ready else "Not Configured 🔴")
st.metric("Instance ID", INSTANCE_ID)

if pending_audits > 0:
    st.warning(f"⚠️ {pending_audits} signals are in `DELIVERY_AUDIT_REQUIRED` state. Manual Telegram verification needed.")

st.write("---")
st.info("💡 **v77 Streamlit Cloud Edition:** SQLite runtime ledger with automatic pre-full cleanup/compaction, one cached scanner worker, immediate startup scan followed by 5-minute synchronized KuCoin scans, Telegram delivery ledger, and anti-whipsaw score guards. **PostgreSQL and DATABASE_URL are not used.**")

st_autorefresh(interval=120000, key="datarefresh")
