import streamlit as st

# ==============================================================================
# 0. STREAMLIT CONFIG & STABLE RUNTIME IDENTITY
# ==============================================================================
st.set_page_config(page_title="Prime Samaresh Engine v75", page_icon="⚙️", layout="centered")

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
from streamlit_autorefresh import st_autorefresh

try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    psycopg2 = None
    ThreadedConnectionPool = None

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
# 1. SECRETS & CONSTANTS
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DATABASE_URL = os.environ.get("DATABASE_URL") 

COINS = ['ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']
LEASE_DURATION_SEC = 180 # v75 P0 Fix: Extended to safely absorb max backoff limits without fencing conflicts
COOLDOWN_MINUTES = 15  
MAX_DELIVERY_ATTEMPTS = 2 
MAX_TELEGRAM_BACKOFF_SEC = 30 # v75 P0 Fix: Hard cap to prevent infinite sleep / lease expiration
OHLCV_LIMIT = 240 

LEDGER_CLAIMED = "CLAIMED"
LEDGER_SENDING = "SENDING"
LEDGER_SENT = "SENT"
LEDGER_FAILED = "FAILED"
LEDGER_UNKNOWN = "UNKNOWN_DELIVERY"
LEDGER_AUDIT = "DELIVERY_AUDIT_REQUIRED" 

LEADER_LOCK_ID = 5975000 
MIGRATION_LOCK_ID = 5975001 
CLAIM_LOCK_NAMESPACE = 5975002 

# ==============================================================================
# 2. GLOBAL LOCKS, POOLING & HEARTBEAT STATE
# ==============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@st.cache_resource
def get_global_primitives():
    return threading.Lock(), threading.Event(), {
        "last_reconcile": 0.0,
        "last_scan_completed": 0.0, # v75 P1 Fix: Semantically honest naming
        "scan_counter": 0,
        "data_feed_errors": 0, # v75 P1 Fix: Operational monitoring
        "is_leader": False,
        "worker_health": "OFFLINE 🔴"
    }

WORKER_LOCK, STOP_EVENT, SHARED_STATE = get_global_primitives()

@st.cache_resource
def get_db_pool():
    if not DATABASE_URL or ThreadedConnectionPool is None: return None
    try:
        return ThreadedConnectionPool(1, 4, dsn=DATABASE_URL, connect_timeout=5)
    except Exception as e:
        logging.error(f"DB Pool creation failed: {e}")
        return None

DB_POOL = get_db_pool()

@contextmanager
def get_db_connection():
    if DB_POOL is None:
        yield None
        return

    conn = None
    broken = False
    try:
        conn = DB_POOL.getconn()
        yield conn
    except Exception:
        broken = True
        if conn is not None:
            try: conn.rollback()
            except Exception: pass
        raise
    finally:
        if conn is not None:
            if broken or conn.closed:
                try: conn.close()
                except Exception: pass
            else:
                try: conn.rollback() 
                except Exception:
                    try:
                        conn.close()
                        return
                    except Exception: pass
                
                try: DB_POOL.putconn(conn)
                except Exception:
                    try: conn.close()
                    except Exception: pass

# ==============================================================================
# 3. DATABASE MIGRATION & HEALTH CHECK 
# ==============================================================================
def check_db_health():
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 2000;") 
                cur.execute("SELECT 1;")
                return cur.fetchone() is not None
        except Exception: 
            try: conn.rollback()
            except: pass
            return False

def initialize_database():
    with get_db_connection() as conn:
        if not conn:
            logging.error("DB Pool unavailable. Distributed ledger disabled.")
            return False
        
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s);", (MIGRATION_LOCK_ID,))
                
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS signals_ledger (
                        signal_id VARCHAR(64) PRIMARY KEY,
                        symbol VARCHAR(32) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT '{LEDGER_CLAIMED}',
                        claimer VARCHAR(32),
                        lease_expires TIMESTAMPTZ,
                        takeover_count INTEGER NOT NULL DEFAULT 0,
                        claim_attempts INTEGER NOT NULL DEFAULT 0,
                        delivery_attempts INTEGER NOT NULL DEFAULT 0,
                        claim_version BIGINT NOT NULL DEFAULT 0,
                        telegram_message_id BIGINT,
                        telegram_chat_id VARCHAR(64),
                        telegram_response_audit TEXT,
                        sent_at TIMESTAMPTZ,
                        last_error TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.critical(f"Failed to acquire migration lock or create base table: {e}")
            return False

        migrations = [
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS takeover_count INTEGER NOT NULL DEFAULT 0;",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS claim_attempts INTEGER NOT NULL DEFAULT 0;",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS delivery_attempts INTEGER NOT NULL DEFAULT 0;",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS claim_version BIGINT NOT NULL DEFAULT 0;",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT;",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS telegram_chat_id VARCHAR(64);",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS telegram_response_audit TEXT;",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS last_error TEXT;",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();",
            "ALTER TABLE signals_ledger ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();",
        ]
        
        migration_failed = False
        for query in migrations:
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                conn.commit()
            except Exception as e:
                conn.rollback() 
                if "already exists" not in str(e).lower() and "duplicate column" not in str(e).lower():
                    migration_failed = True
                    logging.error(f"Migration failed: {e}")
        
        if migration_failed:
            logging.critical("Database migration incomplete. Worker will not start.")
            return False

        try:
            with conn.cursor() as cur:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ledger_status_lease ON signals_ledger(status, lease_expires);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ledger_claimer ON signals_ledger(claimer);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol_sent ON signals_ledger(symbol, sent_at);")
            conn.commit()
        except Exception:
            conn.rollback()

        logging.info("v75 Zenith Ledger migrated successfully.")
        return True

# ==============================================================================
# 4. RECONCILIATION WORKER
# ==============================================================================
def reconcile_ledger(force=False):
    current_time = time.time()
    if not force and (current_time - SHARED_STATE["last_reconcile"] < 900): return
        
    with get_db_connection() as conn:
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE signals_ledger SET status = '{LEDGER_AUDIT}', updated_at = NOW(), 
                    last_error = 'Reconciled: Marked for AUDIT (Network timeout ambiguity)'
                    WHERE status = '{LEDGER_UNKNOWN}' AND updated_at < NOW() - INTERVAL '15 minutes';
                """)
                if cur.rowcount > 0:
                    logging.critical(f"AUDIT REQUIRED: Reconciled {cur.rowcount} UNKNOWN_DELIVERY signals to {LEDGER_AUDIT}.")
                
                cur.execute(f"""
                    UPDATE signals_ledger SET status = '{LEDGER_AUDIT}', updated_at = NOW(), 
                    last_error = 'Reconciled: Stuck in SENDING state (DB Commit Failure Risk)'
                    WHERE status = '{LEDGER_SENDING}' AND updated_at < NOW() - INTERVAL '5 minutes';
                """)
                if cur.rowcount > 0:
                    logging.critical(f"AUDIT REQUIRED: Reconciled {cur.rowcount} stuck SENDING signals to {LEDGER_AUDIT}.")
                
                conn.commit()
                SHARED_STATE["last_reconcile"] = current_time
        except Exception as e:
            conn.rollback()
            logging.error(f"Reconciliation error: {e}")

def get_pending_audit_count():
    with get_db_connection() as conn:
        if not conn: return 0
        try:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 2000;") 
                cur.execute(f"SELECT COUNT(*) FROM signals_ledger WHERE status = '{LEDGER_AUDIT}';")
                return cur.fetchone()[0]
        except Exception:
            try: conn.rollback()
            except: pass
            return 0

# ==============================================================================
# 5. ATOMIC CLAIMING + PER-SYMBOL TRANSACTION LOCK
# ==============================================================================
def acquire_distributed_lease(signal_id, symbol, direction):
    now = datetime.now(timezone.utc)
    lease_expires = now + timedelta(seconds=LEASE_DURATION_SEC)
    
    with get_db_connection() as conn:
        if not conn: return None
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s, hashtext(%s));", (CLAIM_LOCK_NAMESPACE, symbol))

                sql = f"""
                INSERT INTO signals_ledger (
                    signal_id, symbol, direction, status, claimer, lease_expires, 
                    takeover_count, claim_attempts, delivery_attempts, claim_version, updated_at
                ) 
                SELECT %s, %s, %s, %s, %s, %s, 0, 1, 0, 1, NOW()
                WHERE NOT EXISTS (
                    SELECT 1 FROM signals_ledger 
                    WHERE symbol = %s AND (
                        status = '{LEDGER_AUDIT}'
                        OR (status = '{LEDGER_SENT}' AND sent_at > NOW() - %s::interval)
                        OR (status IN ('{LEDGER_CLAIMED}', '{LEDGER_SENDING}', '{LEDGER_UNKNOWN}') AND updated_at > NOW() - INTERVAL '10 minutes')
                    )
                )
                ON CONFLICT (signal_id) DO UPDATE SET
                    status = '{LEDGER_CLAIMED}', 
                    claimer = EXCLUDED.claimer, 
                    lease_expires = EXCLUDED.lease_expires,
                    takeover_count = CASE WHEN signals_ledger.claimer != EXCLUDED.claimer 
                                          THEN signals_ledger.takeover_count + 1 
                                          ELSE signals_ledger.takeover_count END,
                    claim_attempts = signals_ledger.claim_attempts + 1,
                    claim_version = signals_ledger.claim_version + 1,
                    last_error = NULL, updated_at = NOW()
                WHERE signals_ledger.status = '{LEDGER_FAILED}'
                  AND (signals_ledger.lease_expires IS NULL OR signals_ledger.lease_expires < %s)
                  AND signals_ledger.delivery_attempts < %s
                RETURNING signal_id, claim_version, lease_expires, claim_attempts, takeover_count;
                """
                
                cur.execute(sql, (
                    signal_id, symbol, direction, LEDGER_CLAIMED, INSTANCE_ID, lease_expires,
                    symbol, f"{COOLDOWN_MINUTES} minutes",
                    now, MAX_DELIVERY_ATTEMPTS
                ))
                row = cur.fetchone()
                conn.commit()
                if not row: return None
                return {"signal_id": row[0], "claim_version": row[1], "lease_expires": row[2]}
        except Exception as e:
            conn.rollback()
            logging.error(f"Atomic claim error: {e}")
            return None

def transition_to_sending(signal_id, claim_version):
    now = datetime.now(timezone.utc)
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE signals_ledger SET status = '{LEDGER_SENDING}', delivery_attempts = delivery_attempts + 1, updated_at = NOW()
                    WHERE signal_id = %s AND claimer = %s AND claim_version = %s
                      AND status = '{LEDGER_CLAIMED}' AND lease_expires > %s;
                """, (signal_id, INSTANCE_ID, claim_version, now))
                updated = cur.rowcount
                conn.commit()
                return updated == 1
        except Exception:
            conn.rollback()
            return False

def commit_signal_state(signal_id, claim_version, target_status, message_id=None, chat_id=None, response_audit=None, error_message=None):
    with get_db_connection() as conn:
        if not conn: return False
        try:
            with conn.cursor() as cur:
                audit_data = json.dumps(response_audit) if response_audit else None
                if target_status == LEDGER_SENT:
                    cur.execute(f"""
                        UPDATE signals_ledger SET status = '{LEDGER_SENT}', telegram_message_id = %s, telegram_chat_id = %s, telegram_response_audit = %s, sent_at = NOW(), updated_at = NOW()
                        WHERE signal_id = %s AND claimer = %s AND claim_version = %s AND status = '{LEDGER_SENDING}';
                    """, (message_id, str(chat_id), audit_data, signal_id, INSTANCE_ID, claim_version))
                else:
                    cur.execute(f"""
                        UPDATE signals_ledger SET status = %s, last_error = %s, telegram_response_audit = %s, updated_at = NOW()
                        WHERE signal_id = %s AND claimer = %s AND claim_version = %s AND status = '{LEDGER_SENDING}';
                    """, (target_status, str(error_message)[:2000] if error_message else None, audit_data, signal_id, INSTANCE_ID, claim_version))
                updated = cur.rowcount
                conn.commit()
                return updated == 1
        except Exception:
            conn.rollback()
            return False

# ==============================================================================
# 6. SMC LIFECYCLE ENGINE
# ==============================================================================
def fetch_ohlcv(exchange, symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=OHLCV_LIMIT)
        if not ohlcv:
            logging.warning(f"Empty OHLCV response for {symbol} {timeframe}")
            SHARED_STATE["data_feed_errors"] += 1
            return None
            
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        
        exchange_time_ms = exchange.milliseconds()
        
        tf_minutes = 5 
        if timeframe.endswith('m'): tf_minutes = int(timeframe.replace('m', ''))
        elif timeframe.endswith('h'): tf_minutes = int(timeframe.replace('h', '')) * 60
        elif timeframe.endswith('d'): tf_minutes = int(timeframe.replace('d', '')) * 1440
        elif timeframe.endswith('w'): tf_minutes = int(timeframe.replace('w', '')) * 10080
            
        last_open_ms = int(df['timestamp'].iloc[-1].timestamp() * 1000)
        tf_ms = tf_minutes * 60 * 1000
        
        if last_open_ms + tf_ms > exchange_time_ms:
            df = df.iloc[:-1].copy()
            
        return df
    except Exception as e:
        logging.error(f"Fetch error {symbol} {timeframe}: {e}")
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

    tr = np.maximum.reduce([
        high - low,
        np.abs(high - prev_close),
        np.abs(low - prev_close)
    ])
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
            if not is_invalidated:
                valid_bull_fvgs.append(fvg)
        active_bull_fvgs = valid_bull_fvgs
        
        valid_bear_fvgs = []
        for fvg in active_bear_fvgs:
            is_tapped = (h >= fvg['bot']) and (l <= fvg['top'])
            is_invalidated = c > fvg['top']
            if is_tapped:
                if fvg['taps'] == 0: fvg_tapped_bear[i] = True
                fvg['taps'] += 1
            if not is_invalidated:
                valid_bear_fvgs.append(fvg)
        active_bear_fvgs = valid_bear_fvgs

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
    if len(df_1h) < 210 or len(df_15m) < 210 or len(df_5m) < 210:
        return 0, False

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
# 8. TELEGRAM PIPELINE (v75 P0 FIX: SAFE LEASE & 429 HANDLING)
# ==============================================================================
def distributed_signal_pipeline(symbol, direction, score, btc_regime, df_5m, atr_pos, display_icon):
    closed_ts_ms = int((df_5m['timestamp'].iloc[-1] + pd.Timedelta(minutes=5)).timestamp() * 1000)
    sig_string = f"{symbol}|{closed_ts_ms}|{direction}"
    signal_id = hashlib.sha256(sig_string.encode()).hexdigest() 
    
    closed_timestamp_str = (df_5m['timestamp'].iloc[-1] + pd.Timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')

    claim = acquire_distributed_lease(signal_id, symbol, direction)
    if not claim: return False
    claim_version = claim["claim_version"]
    
    if not transition_to_sending(signal_id, claim_version):
        logging.warning(f"Lost fencing ownership before Telegram send: {signal_id[:16]}")
        return False

    msg = (
        f"⚡ <b>Prime Samaresh Engine v75</b>\n"
        f"🪙 <b>Pair:</b> {symbol}\n"
        f"📈 <b>Action:</b> {direction} {display_icon}\n"
        f"🔥 <b>Conviction Score:</b> {score}/100\n"
        f"⚖️ <b>BTC Regime:</b> {btc_regime}\n"
        f"⏱️ <b>Candle Close:</b> {closed_timestamp_str} UTC\n"
        f"📊 <b>ATR Position:</b> {atr_pos:.2f}\n"
        f"🆔 <b>Signal ID:</b> <code>{signal_id[:16]}...</code>"
    )
    
    def attempt_telegram_delivery():
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        return TELEGRAM_SESSION.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
    
    try:
        # v75 P0 Fix: Fast-fail FAILED classification on missing credentials (no ambiguity)
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            commit_signal_state(signal_id, claim_version, LEDGER_FAILED, error_message="Fatal: Telegram credentials missing")
            return False
            
        response = attempt_telegram_delivery()
        
        # v75 P0 Fix: Enforced Hard Cap on 429 Retry-After to prevent lease expiration fencing failures
        if response.status_code == 429:
            try: retry_data = response.json()
            except: retry_data = {}
            retry_after = retry_data.get("parameters", {}).get("retry_after", 5.0)
            
            if retry_after > MAX_TELEGRAM_BACKOFF_SEC:
                commit_signal_state(signal_id, claim_version, LEDGER_FAILED, response_audit=retry_data, error_message=f"Rate Limited: Retry-After ({retry_after}s) exceeds MAX_BACKOFF ({MAX_TELEGRAM_BACKOFF_SEC}s)")
                return False
                
            logging.warning(f"Telegram Rate Limit (429) hit. Backing off for {retry_after}s.")
            time.sleep(retry_after)
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
                
            if committed:
                logging.info(f"Signal delivered & committed (Msg ID: {msg_id}): {signal_id[:16]}")
                return True
            else:
                logging.critical(f"FATAL: Telegram DELIVERED (Msg ID: {msg_id}) but DB Commit FAILED. Signal {signal_id[:16]} quarantined in SENDING.")
                return False
                
        else:
            status_code = response.status_code
            error_msg = f"HTTP {status_code}: {response.text[:200]}"
            
            if status_code in [400, 401, 403, 404]:
                commit_signal_state(signal_id, claim_version, LEDGER_FAILED, response_audit=data, error_message=f"Permanent Failure: {error_msg}")
            elif status_code == 429:
                commit_signal_state(signal_id, claim_version, LEDGER_FAILED, response_audit=data, error_message=f"Rate Limited (Double hit): {error_msg}")
            elif status_code >= 500:
                commit_signal_state(signal_id, claim_version, LEDGER_UNKNOWN, response_audit=data, error_message=f"Server Error (Ambiguous): {error_msg}")
            else:
                commit_signal_state(signal_id, claim_version, LEDGER_UNKNOWN, response_audit=data, error_message=f"Unknown Error: {error_msg}")
                
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
        
        if bull_valid and not bear_valid:
            direction, score, display_icon = "BULL", bull_score, "🟢"
        elif bear_valid and not bull_valid:
            direction, score, display_icon = "BEAR", bear_score, "🔴"
        elif bull_valid and bear_valid:
            if bull_score > bear_score: direction, score, display_icon = "BULL", bull_score, "🟢"
            elif bear_score > bull_score: direction, score, display_icon = "BEAR", bear_score, "🔴"
            else: direction = None
            
        if direction:
            atr_pos = df_5m['atr_position'].iloc[-1]
            distributed_signal_pipeline(symbol, direction, score, btc_regime, df_5m, atr_pos, display_icon)
        
        del df_1h, df_15m, df_5m
        
    SHARED_STATE["last_scan_completed"] = time.time()
    SHARED_STATE["scan_counter"] += 1
    
    if SHARED_STATE["scan_counter"] % 6 == 0:
        gc.collect()

# ==============================================================================
# 10. WORKER LIFECYCLE 
# ==============================================================================
def acquire_leader_lock(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s);", (LEADER_LOCK_ID,))
            return cur.fetchone()[0]
    except Exception as e:
        logging.error(f"Advisory lock attempt error: {e}")
        return False

def ping_leader_conn(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            return True
    except Exception: return False

def get_next_scan_time():
    now = datetime.now(timezone.utc)
    remainder = 5 - (now.minute % 5)
    next_boundary = now + timedelta(minutes=remainder)
    return next_boundary.replace(second=0, microsecond=0).timestamp()

def scanner_worker_loop():
    logging.info(f"Prime Samaresh Engine v75 Worker Started (ID: {INSTANCE_ID})")
    
    if not initialize_database():
        logging.critical("Database initialization failed. Worker shutting down.")
        SHARED_STATE["worker_health"] = "DB SETUP FAILED 🔴"
        return

    # v75 P1 Fix: Immediate startup reconciliation sweeps any pre-existing stuck states safely into AUDIT
    reconcile_ledger(force=True)

    leader_conn = None
    is_leader = False
    exchange = None
    
    while not STOP_EVENT.is_set():
        try:
            next_scan = get_next_scan_time()
            sleep_duration = max(0.0, next_scan - time.time()) + random.uniform(2.0, 7.0)
            if STOP_EVENT.wait(sleep_duration): break 
            
            if not check_db_health():
                logging.error("Database health check failed. Skipping scan cycle.")
                is_leader = False
                SHARED_STATE["is_leader"] = False
                SHARED_STATE["worker_health"] = "DB UNREACHABLE 🔴"
                if leader_conn:
                    try: leader_conn.close()
                    except: pass
                    leader_conn = None
                continue

            if not is_leader:
                if leader_conn is None or not ping_leader_conn(leader_conn):
                    try:
                        if leader_conn: leader_conn.close()
                        leader_conn = psycopg2.connect(DATABASE_URL, connect_timeout=5, options="-c statement_timeout=5000")
                        leader_conn.autocommit = True
                    except Exception as e:
                        logging.error(f"Failed to connect for leader election: {e}")
                        leader_conn = None
                        SHARED_STATE["worker_health"] = "ELECTION FAILED 🔴"
                        continue
                
                if leader_conn and acquire_leader_lock(leader_conn):
                    is_leader = True
                    SHARED_STATE["is_leader"] = True
                    SHARED_STATE["worker_health"] = "HEALTHY 🟢"
                    logging.info("Successfully acquired Leader Advisory Lock. Activating Scanner.")
                else:
                    SHARED_STATE["worker_health"] = "HEALTHY 🟢"
                    continue 
            else:
                if not ping_leader_conn(leader_conn):
                    logging.warning("Leader connection lost. Re-entering election state.")
                    is_leader = False
                    SHARED_STATE["is_leader"] = False
                    SHARED_STATE["worker_health"] = "LEADER LOST 🟡"
                    try: leader_conn.close()
                    except: pass
                    leader_conn = None
                    continue

            if exchange is None:
                exchange = ccxt.kucoin({'enableRateLimit': True, 'timeout': 15000})
            
            run_scan_cycle(exchange)
            
            # v75 P1 Fix: Real-time Data Feed health translation 
            if SHARED_STATE["data_feed_errors"] > 0:
                SHARED_STATE["worker_health"] = "DATA FEED DOWN 🟡"
            else:
                SHARED_STATE["worker_health"] = "HEALTHY 🟢"
            
        except Exception as e:
            logging.error(f"Worker loop crash: {e}")
            if exchange:
                try: exchange.close()
                except: pass
                exchange = None
            is_leader = False
            SHARED_STATE["is_leader"] = False
            SHARED_STATE["worker_health"] = "CRASHED 🔴"
            if leader_conn:
                try: leader_conn.close()
                except: pass
                leader_conn = None
            STOP_EVENT.wait(5)
            
    if exchange:
        try: exchange.close()
        except: pass
    if leader_conn:
        try: leader_conn.close()
        except: pass

def start_background_scanner():
    with WORKER_LOCK:
        for t in threading.enumerate():
            if t.name == "PrimeSamareshScanner" and t.is_alive(): return False
        try:
            worker = threading.Thread(target=scanner_worker_loop, name="PrimeSamareshScanner", daemon=True)
            worker.start()
            return True
        except Exception as e:
            logging.error(f"Failed to start worker thread: {e}")
            return False

# ==============================================================================
# 11. STREAMLIT UI
# ==============================================================================
st.markdown("## Prime Samaresh Engine v75")
st.markdown("**The Zenith Master Build: Uncompromising Delivery & Ledger Durability**")

start_background_scanner()

db_healthy = check_db_health()
pending_audits = get_pending_audit_count()

worker_health = SHARED_STATE["worker_health"]
is_leader = SHARED_STATE["is_leader"]
last_scan = SHARED_STATE["last_scan_completed"]

if not any(t.name == "PrimeSamareshScanner" and t.is_alive() for t in threading.enumerate()):
    worker_health = "OFFLINE 🔴"
elif last_scan > 0 and time.time() - last_scan > 600:
    worker_health = "DEGRADED 🟡"

role_display = "LEADER 👑" if is_leader else "REPLICA 🛡️"
last_scan_display = datetime.fromtimestamp(last_scan, timezone.utc).strftime('%H:%M:%S UTC') if last_scan > 0 else "Pending..."

st.metric("Engine Status", "Online 🟢" if worker_health not in ["OFFLINE 🔴", "CRASHED 🔴", "DB UNREACHABLE 🔴", "DB SETUP FAILED 🔴"] else "Offline 🔴")
col1, col2, col3 = st.columns(3)
col1.metric("Worker Health", worker_health)
col2.metric("Current Role", role_display)
col3.metric("Last Completed Scan", last_scan_display)

st.write("---")
st.metric("Database Connectivity", "Connected 🟢" if db_healthy else "Unreachable 🔴 (Scan Suspended)")
st.metric("Instance ID", INSTANCE_ID)

if pending_audits > 0:
    st.warning(f"⚠️ {pending_audits} signals are in `DELIVERY_AUDIT_REQUIRED` state. Manual Telegram verification needed.")

st.write("---")
st.info("💡 **v75 Final Architectural State:** Employs an exact HTTP 4xx/5xx state machine for fault-tolerant Telegram dispatching, secures PostgreSQL schema mutations via singleton Advisory Migration Locks, mathematically decouples displacement baselines from current active thresholds, hard-caps external API backoffs to safeguard internal 180s lease durations, and executes fail-safe startup reconciliations establishing an irrefutably durable Signal Ledger.")

st_autorefresh(interval=120000, key="datarefresh")
