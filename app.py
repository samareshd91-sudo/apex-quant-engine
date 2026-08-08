import streamlit as st

# ==============================================================================
# 0. STREAMLIT CONFIG
# ==============================================================================
st.set_page_config(page_title="Prime Samaresh Engine v59.7", page_icon="⚙️", layout="centered")

import ccxt
import pandas as pd
import numpy as np
import time
import threading
import logging
import requests
import os
import gc
import psutil
import uuid
import hashlib
import random
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

try:
    import psycopg2
except ImportError:
    psycopg2 = None

# ==============================================================================
# 1. SECRETS & IDENTITY
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DATABASE_URL = os.environ.get("DATABASE_URL") 

COINS = ['ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']
INSTANCE_ID = str(uuid.uuid4())[:8]
LEASE_DURATION_SEC = 30 
MAX_ATTEMPTS = 3 # 1 Initial + 2 Retries

# ==============================================================================
# 2. GLOBAL LOCKS & LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@st.cache_resource
def get_global_primitives():
    return threading.Lock(), threading.Event()

WORKER_LOCK, STOP_EVENT = get_global_primitives()

# ==============================================================================
# 3. DATABASE MIGRATION (TIMESTAMPTZ COMPATIBLE)
# ==============================================================================
def initialize_database():
    if not DATABASE_URL or psycopg2 is None:
        logging.error("DATABASE_URL missing. Distributed ledger will NOT work.")
        return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signals_ledger (
                        signal_id VARCHAR(64) PRIMARY KEY,
                        symbol VARCHAR(32) NOT NULL,
                        direction VARCHAR(10) NOT NULL,
                        status VARCHAR(16) NOT NULL,
                        claimer VARCHAR(32),
                        lease_expires TIMESTAMPTZ,
                        attempts INTEGER NOT NULL DEFAULT 1,
                        sent_at TIMESTAMPTZ
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_status_expires 
                    ON signals_ledger(status, lease_expires);
                """)
                conn.commit()
        logging.info("Distributed Ledger Schema Verified Successfully.")
    except Exception as e:
        logging.error(f"Migration Error: {e}")

# ==============================================================================
# 4. LEASE-BASED IDEMPOTENT DELIVERY WITH FAIL-CLOSED GATING
# ==============================================================================
def acquire_distributed_lease(signal_id, symbol, direction):
    if not DATABASE_URL or psycopg2 is None: 
        logging.error("Distributed ledger unavailable — signal delivery blocked (Fail-closed).")
        return False 
    
    current_time_utc = datetime.now(timezone.utc)
    lease_expires_utc = current_time_utc + timedelta(seconds=LEASE_DURATION_SEC)
    
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                sql = """
                INSERT INTO signals_ledger (signal_id, symbol, direction, status, claimer, lease_expires, attempts)
                VALUES (%s, %s, %s, 'PENDING', %s, %s, 1)
                ON CONFLICT (signal_id) DO UPDATE SET
                    status = 'PENDING',
                    claimer = EXCLUDED.claimer,
                    lease_expires = EXCLUDED.lease_expires,
                    attempts = signals_ledger.attempts + 1
                WHERE (signals_ledger.status = 'FAILED' AND signals_ledger.attempts < %s)
                   OR (signals_ledger.status = 'PENDING' AND signals_ledger.lease_expires < %s AND signals_ledger.attempts < %s)
                RETURNING status;
                """
                cur.execute(sql, (signal_id, symbol, direction, INSTANCE_ID, lease_expires_utc, MAX_ATTEMPTS, current_time_utc, MAX_ATTEMPTS))
                result = cur.fetchone()
                conn.commit()
                return result is not None 
    except Exception as e:
        logging.error(f"DB Lock Error: {e}")
        return False 

def commit_signal_state(signal_id, status):
    if not DATABASE_URL or psycopg2 is None: 
        return False
        
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE signals_ledger 
                    SET status = %s, sent_at = NOW() 
                    WHERE signal_id = %s AND claimer = %s
                """, (status, signal_id, INSTANCE_ID))
                updated = cur.rowcount
                conn.commit()
                return updated == 1
    except Exception as e:
        logging.error(f"DB Commit Error: {e}")
        return False

def distributed_signal_pipeline(symbol, direction, score, btc_regime, closed_timestamp, atr_pct, display_icon):
    sig_string = f"{symbol}_{closed_timestamp}_{direction}"
    signal_id = hashlib.sha256(sig_string.encode()).hexdigest()[:16]
    
    if not acquire_distributed_lease(signal_id, symbol, direction):
        return False 
            
    msg = (
        f"⚡ <b>Prime Samaresh Engine v59.7</b>\n"
        f"🪙 <b>Pair:</b> {symbol}\n"
        f"📈 <b>Action:</b> {direction} {display_icon}\n"
        f"🔥 <b>Conviction Score:</b> {score}/100\n"
        f"⚖️ <b>BTC Regime:</b> {btc_regime}\n"
        f"⏱️ <b>Closed TF:</b> {closed_timestamp} (UTC)\n"
        f"📊 <b>ATR Percentile:</b> {atr_pct:.2f}"
    )
    
    telegram_ok = False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
        telegram_ok = resp.ok
    except: pass
    
    if telegram_ok:
        if commit_signal_state(signal_id, "SENT"):
            return True
        logging.error(f"Telegram sent but ledger commit failed (Phantom Delivery possible): {signal_id}")
        return False
    else:
        commit_signal_state(signal_id, "FAILED")
        return False

# ==============================================================================
# 5. SMC LIFECYCLE ENGINE (NATIVE INDICATORS)
# ==============================================================================
def fetch_ohlcv(exchange, symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=301)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df.iloc[:-1].copy() 
    except Exception as e:
        logging.error(f"Fetch error {symbol} {timeframe}: {e}")
        return None

def calculate_smc_features(df):
    df['ema_50'] = df['close'].ewm(span=50, adjust=False, min_periods=50).mean()
    df['ema_200'] = df['close'].ewm(span=200, adjust=False, min_periods=200).mean()
    
    prev_close = df['close'].shift(1)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - prev_close).abs(),
        (df['low'] - prev_close).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.ewm(alpha=1/14, adjust=False, min_periods=14).mean()

    atr_min = df['atr'].rolling(window=100, min_periods=50).min()
    atr_max = df['atr'].rolling(window=100, min_periods=50).max()
    df['atr_percentile'] = ((df['atr'] - atr_min) / (atr_max - atr_min).replace(0, np.nan)).fillna(0.5)

    df['is_swing_high'] = (df['high'].shift(2) > df['high'].shift(1)) & (df['high'].shift(2) > df['high']) & (df['high'].shift(2) > df['high'].shift(3)) & (df['high'].shift(2) > df['high'].shift(4))
    df['is_swing_low'] = (df['low'].shift(2) < df['low'].shift(1)) & (df['low'].shift(2) < df['low']) & (df['low'].shift(2) < df['low'].shift(3)) & (df['low'].shift(2) < df['low'].shift(4))
    
    df['swing_high_val'] = np.where(df['is_swing_high'], df['high'].shift(2), np.nan)
    df['swing_high_val'] = df['swing_high_val'].ffill()
    df['swing_low_val'] = np.where(df['is_swing_low'], df['low'].shift(2), np.nan)
    df['swing_low_val'] = df['swing_low_val'].ffill()

    closes, opens, highs, lows = df['close'].values, df['open'].values, df['high'].values, df['low'].values
    swing_highs, swing_lows = df['swing_high_val'].values, df['swing_low_val'].values
    
    n = len(df)
    initial_break_bull, initial_break_bear = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    bos_bull, choch_bull, bos_bear, choch_bear = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool), np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    fvg_tapped_bull, fvg_tapped_bear = np.zeros(n, dtype=bool), np.zeros(n, dtype=bool)
    
    trend = 0 
    active_bull_fvgs, active_bear_fvgs = [], []
    
    for i in range(2, n):
        c, o, h, l = closes[i], opens[i], highs[i], lows[i]
        prev_sh, prev_sl = swing_highs[i-1], swing_lows[i-1]
        
        if not np.isnan(prev_sh) and c > prev_sh:
            if trend == 0: initial_break_bull[i] = True
            elif trend == -1: choch_bull[i] = True
            elif trend == 1: bos_bull[i] = True
            trend = 1
            
        if not np.isnan(prev_sl) and c < prev_sl:
            if trend == 0: initial_break_bear[i] = True
            elif trend == 1: choch_bear[i] = True
            elif trend == -1: bos_bear[i] = True
            trend = -1 
            
        if l > highs[i-2] and c > o:
            active_bull_fvgs.append({'idx': i, 'top': l, 'bot': highs[i-2], 'taps': 0})
        if h < lows[i-2] and c < o:
            active_bear_fvgs.append({'idx': i, 'top': lows[i-2], 'bot': h, 'taps': 0})
            
        valid_bull_fvgs = []
        for fvg in active_bull_fvgs:
            if c < fvg['bot']: continue 
            valid_bull_fvgs.append(fvg)
        active_bull_fvgs = valid_bull_fvgs
        
        if active_bull_fvgs:
            latest_fvg = active_bull_fvgs[-1] 
            if l <= latest_fvg['top'] and c >= latest_fvg['bot']: 
                if latest_fvg['taps'] == 0: fvg_tapped_bull[i] = True 
                latest_fvg['taps'] += 1

        valid_bear_fvgs = []
        for fvg in active_bear_fvgs:
            if c > fvg['top']: continue 
            valid_bear_fvgs.append(fvg)
        active_bear_fvgs = valid_bear_fvgs
        
        if active_bear_fvgs:
            latest_fvg = active_bear_fvgs[-1]
            if h >= latest_fvg['bot'] and c <= latest_fvg['top']:
                if latest_fvg['taps'] == 0: fvg_tapped_bear[i] = True
                latest_fvg['taps'] += 1

    df['initial_break_bull'], df['bos_bull'], df['choch_bull'] = initial_break_bull, bos_bull, choch_bull
    df['initial_break_bear'], df['bos_bear'], df['choch_bear'] = initial_break_bear, bos_bear, choch_bear
    df['fvg_tapped_bull'], df['fvg_tapped_bear'] = fvg_tapped_bull, fvg_tapped_bear

    df['liq_sweep_bull'] = (df['low'] < df['swing_low_val'].shift(1)) & (df['close'] > df['swing_low_val'].shift(1))
    df['liq_sweep_bear'] = (df['high'] > df['swing_high_val'].shift(1)) & (df['close'] < df['swing_high_val'].shift(1))
    
    df['candle_range'] = df['high'] - df['low']
    df['body_size'] = abs(df['close'] - df['open'])
    avg_body, avg_vol = df['body_size'].rolling(20).mean(), df['volume'].rolling(20).mean()
    
    df['is_displacement'] = (df['body_size'] > df['candle_range'] * 0.6) & (df['body_size'] > avg_body) & (df['volume'] > avg_vol)
    df['displacement_bull'] = df['is_displacement'] & (df['close'] > df['open'])
    df['displacement_bear'] = df['is_displacement'] & (df['close'] < df['open'])
    df['orderflow_bull'] = (df['close'] > df['open']) & (df['volume'] > avg_vol * 1.5)
    df['orderflow_bear'] = (df['close'] < df['open']) & (df['volume'] > avg_vol * 1.5)

    return df

def analyze_btc_regime(exchange):
    btc_1h = fetch_ohlcv(exchange, 'BTC/USDT', '1h')
    if btc_1h is None or btc_1h.empty: return "NEUTRAL"
    btc_1h = calculate_smc_features(btc_1h)
    
    c1 = btc_1h.iloc[-1] 
    res = "NEUTRAL"
    if c1['ema_50'] > c1['ema_200'] and c1['close'] > c1['ema_50']: res = "STRONG_BULL" if c1['orderflow_bull'] else "BULL"
    elif c1['ema_50'] < c1['ema_200'] and c1['close'] < c1['ema_50']: res = "STRONG_BEAR" if c1['orderflow_bear'] else "BEAR"
    del btc_1h 
    return res

# ==============================================================================
# 6. DETERMINISTIC SCORE ENGINE
# ==============================================================================
def evaluate_engine_score(df_1h, df_15m, df_5m, direction, btc_regime):
    c1h, c15m, c5m = df_1h.iloc[-1], df_15m.iloc[-1], df_5m.iloc[-1]
    cat_htf, cat_liq, cat_fvg, cat_regime = 0, 0, 0, 0
    
    if direction == "BULL":
        if c1h['close'] > c1h['ema_50']: cat_htf += 15
        if c1h['ema_50'] > c1h['ema_200']: cat_htf += 15
        
        if c15m['liq_sweep_bull'] or c5m['liq_sweep_bull']: cat_liq += 10
        if c15m['bos_bull'] or c5m['bos_bull'] or c15m['initial_break_bull']: cat_liq += 10
        elif c15m['choch_bull'] or c5m['choch_bull']: cat_liq += 10
        if c5m['displacement_bull']: cat_liq += 5
        
        if c5m['fvg_tapped_bull']: cat_fvg += 15
        if c5m['orderflow_bull']: cat_fvg += 10
        
        if btc_regime in ["BULL", "STRONG_BULL"]: cat_regime += 20
            
    elif direction == "BEAR":
        if c1h['close'] < c1h['ema_50']: cat_htf += 15
        if c1h['ema_50'] < c1h['ema_200']: cat_htf += 15
        
        if c15m['liq_sweep_bear'] or c5m['liq_sweep_bear']: cat_liq += 10
        if c15m['bos_bear'] or c5m['bos_bear'] or c15m['initial_break_bear']: cat_liq += 10
        elif c15m['choch_bear'] or c5m['choch_bear']: cat_liq += 10
        if c5m['displacement_bear']: cat_liq += 5
        
        if c5m['fvg_tapped_bear']: cat_fvg += 15
        if c5m['orderflow_bear']: cat_fvg += 10
        
        if btc_regime in ["BEAR", "STRONG_BEAR"]: cat_regime += 20
        
    total_score = min(max(cat_htf + cat_liq + cat_fvg + cat_regime, 0), 100)
    is_valid = (total_score >= 75) and (cat_htf >= 15) and (cat_liq >= 10) and (cat_fvg >= 15)
    return total_score, is_valid

# ==============================================================================
# 7. SCANNER CYCLE
# ==============================================================================
def run_scan_cycle(exchange):
    btc_regime = analyze_btc_regime(exchange)

    for symbol in COINS:
        df_1h = fetch_ohlcv(exchange, symbol, '1h')
        df_15m = fetch_ohlcv(exchange, symbol, '15m')
        df_5m = fetch_ohlcv(exchange, symbol, '5m')
        
        if any(df is None or df.empty for df in [df_1h, df_15m, df_5m]): continue
        
        df_1h = calculate_smc_features(df_1h)
        df_15m = calculate_smc_features(df_15m)
        df_5m = calculate_smc_features(df_5m)
        
        bull_score, bull_valid = evaluate_engine_score(df_1h, df_15m, df_5m, "BULL", btc_regime) if btc_regime != "STRONG_BEAR" else (0, False)
        bear_score, bear_valid = evaluate_engine_score(df_1h, df_15m, df_5m, "BEAR", btc_regime) if btc_regime != "STRONG_BULL" else (0, False)
        
        direction, score, display_icon = None, 0, ""
        if bull_valid and bear_valid:
            pass 
        elif bull_valid:
            direction, score, display_icon = "BULL", bull_score, "🟢"
        elif bear_valid:
            direction, score, display_icon = "BEAR", bear_score, "🔴"
            
        if direction:
            closed_timestamp = df_5m['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
            atr_pct = df_5m['atr_percentile'].iloc[-1]
            distributed_signal_pipeline(symbol, direction, score, btc_regime, closed_timestamp, atr_pct, display_icon)
        
        del df_1h, df_15m, df_5m 

# ==============================================================================
# 8. WORKER LIFECYCLE
# ==============================================================================
def get_next_scan_time():
    now = datetime.now(timezone.utc)
    remainder = 5 - (now.minute % 5)
    next_boundary = now + timedelta(minutes=remainder)
    next_boundary = next_boundary.replace(second=0, microsecond=0)
    return next_boundary.timestamp()

def scanner_worker_loop():
    logging.info(f"Prime Samaresh Engine v59.7 Worker Started (ID: {INSTANCE_ID})")
    exchange = None
    
    initialize_database()
    
    while not STOP_EVENT.is_set():
        try:
            next_scan = get_next_scan_time()
            jitter = random.uniform(5.0, 15.0) 
            sleep_duration = max(0.0, next_scan - time.time()) + jitter
            
            if STOP_EVENT.wait(sleep_duration): 
                break 
                
            if exchange is None: exchange = ccxt.kucoin({'enableRateLimit': True, 'timeout': 15000})
            run_scan_cycle(exchange)
            gc.collect() 
            
        except Exception as e:
            logging.error(f"Worker crashed: {e}")
            if exchange:
                try: exchange.close()
                except: pass
                exchange = None
            STOP_EVENT.wait(5)
            
    if exchange:
        try: exchange.close()
        except: pass

def start_background_scanner():
    with WORKER_LOCK:
        for t in threading.enumerate():
            if t.name == "PrimeSamareshScanner" and t.is_alive(): return False
        try:
            worker = threading.Thread(target=scanner_worker_loop, name="PrimeSamareshScanner", daemon=True)
            worker.start()
            return True
        except: return False

# ==============================================================================
# 9. STREAMLIT UI
# ==============================================================================
st.markdown("## Prime Samaresh Engine v59.7")
st.markdown("**Strict Distributed Ledger Architecture (Fail-Closed Execution)**")

start_background_scanner()

engine_status = "Offline 🔴"
for t in threading.enumerate():
    if t.name == "PrimeSamareshScanner" and t.is_alive():
        engine_status = "Online 🟢"
        break

st.write("---")
col1, col2 = st.columns(2)
col1.metric("System Memory (RSS)", f"{psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024):.2f} MB")
col2.metric("Engine Status", engine_status)

db_status = "Connected 🟢" if DATABASE_URL else "Not Configured 🔴 (Delivery Blocked)"
st.metric("Database Status", db_status)
st.metric("Instance ID", INSTANCE_ID)
st.write("---")
st.info("💡 **Note:** To run this distributed worker, a PostgreSQL database MUST be attached via `DATABASE_URL` environment variable.")

st_autorefresh(interval=60000, key="datarefresh")
