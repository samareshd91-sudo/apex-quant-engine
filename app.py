import streamlit as st

# ==============================================================================
# 0. STREAMLIT CONFIG
# ==============================================================================
st.set_page_config(page_title="Prime Samaresh Engine v59.1", page_icon="⚙️", layout="centered")

import ccxt
import pandas as pd
import pandas_ta as ta
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
import psycopg2
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

# ==============================================================================
# 1. SECRETS & IDENTITY
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DATABASE_URL = os.environ.get("DATABASE_URL") # Postgres Connection String

COINS = ['ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT']
INSTANCE_ID = str(uuid.uuid4())[:8]
LEASE_DURATION_SEC = 30 # 30 seconds to complete Telegram send

# ==============================================================================
# 2. GLOBAL LOCKS (SINGLETON)
# ==============================================================================
@st.cache_resource
def get_global_primitives():
    return threading.Lock(), threading.Event()

WORKER_LOCK, STOP_EVENT = get_global_primitives()

# ==============================================================================
# 3. TRUE ATOMIC EVENT LEDGER & LEASE ARCHITECTURE (Postgres)
# ==============================================================================
def acquire_distributed_lease(signal_id, symbol, direction, current_time):
    """v59.1: True Atomic Lock using Postgres ON CONFLICT"""
    if not DATABASE_URL: return False
    
    lease_expires = current_time + LEASE_DURATION_SEC
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Attempt to insert new claim OR take over if FAILED/EXPIRED
                sql = """
                INSERT INTO signals_ledger (signal_id, symbol, direction, status, claimer, lease_expires, attempts)
                VALUES (%s, %s, %s, 'PENDING', %s, %s, 1)
                ON CONFLICT (signal_id) DO UPDATE SET
                    status = 'PENDING',
                    claimer = EXCLUDED.claimer,
                    lease_expires = EXCLUDED.lease_expires,
                    attempts = signals_ledger.attempts + 1
                WHERE signals_ledger.status = 'FAILED'
                   OR (signals_ledger.status = 'PENDING' AND signals_ledger.lease_expires < %s)
                RETURNING status;
                """
                cur.execute(sql, (signal_id, symbol, direction, INSTANCE_ID, lease_expires, current_time))
                result = cur.fetchone()
                conn.commit()
                return result is not None # True if we successfully acquired the lock/lease
    except Exception as e:
        logging.error(f"DB Lock Error: {e}")
        return False

def commit_signal_state(signal_id, status):
    """Updates ledger after Telegram Side-Effect"""
    if not DATABASE_URL: return
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE signals_ledger 
                    SET status = %s, sent_at = NOW() 
                    WHERE signal_id = %s AND claimer = %s
                """, (status, signal_id, INSTANCE_ID))
                conn.commit()
    except Exception as e:
        logging.error(f"DB Commit Error: {e}")

def distributed_signal_pipeline(symbol, direction, score, btc_regime, closed_timestamp, atr_pct, display_icon):
    """v59.1: Effectively-Once Idempotent Delivery Layer"""
    sig_string = f"{symbol}_{closed_timestamp}_{direction}"
    signal_id = hashlib.sha256(sig_string.encode()).hexdigest()[:16]
    current_time = time.time()
    
    # 1. ATOMIC CLAIM (No more race conditions!)
    if not acquire_distributed_lease(signal_id, symbol, direction, current_time):
        return False # Lost race, or already sent, or lease currently held by another instance
            
    # 2. EXTERNAL SIDE EFFECT (TELEGRAM)
    msg = (
        f"⚡ <b>Prime Samaresh Engine v59.1</b>\n"
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
    
    # 3. EVENT LEDGER COMMIT
    if telegram_ok:
        commit_signal_state(signal_id, "SENT")
        return True
    else:
        commit_signal_state(signal_id, "FAILED") # Releases lease implicitly for retries
        return False

# ==============================================================================
# 4. V59.1 TRUE SMC LIFECYCLE ENGINE
# ==============================================================================
def fetch_ohlcv(exchange, symbol, timeframe):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=301)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df.iloc[:-1].copy() # Explicit Closed Candle
    except Exception as e:
        logging.error(f"Fetch error {symbol} {timeframe}: {e}")
        return None

def calculate_smc_features(df):
    """v59.1: Proper UNKNOWN->BREAK State Machine & Active FVG Lifecycle"""
    df['ema_50'] = ta.ema(df['close'], length=50)
    df['ema_200'] = ta.ema(df['close'], length=200)
    
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
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
    
    trend = 0 # 0=UNKNOWN, 1=BULLISH, -1=BEARISH
    active_bull_fvgs = [] # Track real objects: {idx, top, bot, taps}
    active_bear_fvgs = []
    
    for i in range(2, n):
        c, o, h, l = closes[i], opens[i], highs[i], lows[i]
        prev_sh, prev_sl = swing_highs[i-1], swing_lows[i-1]
        
        # --- A. True SMC Structure Machine ---
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
            
        # --- B. Strict Active FVG Lifecycle ---
        # Creation
        if l > highs[i-2] and c > o:
            active_bull_fvgs.append({'idx': i, 'top': l, 'bot': highs[i-2], 'taps': 0})
        if h < lows[i-2] and c < o:
            active_bear_fvgs.append({'idx': i, 'top': lows[i-2], 'bot': h, 'taps': 0})
            
        # Evaluation & Mitigation (Only evaluate the LATEST valid FVG)
        # Bullish
        valid_bull_fvgs = []
        for fvg in active_bull_fvgs:
            if c < fvg['bot']: continue # Invalidated
            valid_bull_fvgs.append(fvg)
        active_bull_fvgs = valid_bull_fvgs
        
        if active_bull_fvgs:
            latest_fvg = active_bull_fvgs[-1] # Focus ONLY on the most recent uninvalidated FVG
            if l <= latest_fvg['top'] and c >= latest_fvg['bot']: # Tap + Hold
                if latest_fvg['taps'] == 0:
                    fvg_tapped_bull[i] = True # Signal ONLY on FIRST tap of the LATEST active FVG
                latest_fvg['taps'] += 1

        # Bearish
        valid_bear_fvgs = []
        for fvg in active_bear_fvgs:
            if c > fvg['top']: continue 
            valid_bear_fvgs.append(fvg)
        active_bear_fvgs = valid_bear_fvgs
        
        if active_bear_fvgs:
            latest_fvg = active_bear_fvgs[-1]
            if h >= latest_fvg['bot'] and c <= latest_fvg['top']:
                if latest_fvg['taps'] == 0:
                    fvg_tapped_bear[i] = True
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
# 5. DETERMINISTIC SCORE ENGINE
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
        
        # Neutral = 0 Points (Confirmation requirement)
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
# 6. SCANNER CYCLE
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
            pass # Mutual Suppression
        elif bull_valid:
            direction, score, display_icon = "BULL", bull_score, "🟢"
        elif bear_valid:
            direction, score, display_icon = "BEAR", bear_score, "🔴"
            
        if direction:
            closed_timestamp = df_5m['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
            atr_pct = df_5m['atr_percentile'].iloc[-1]
            # TRUE ATOMIC PIPELINE
            distributed_signal_pipeline(symbol, direction, score, btc_regime, closed_timestamp, atr_pct, display_icon)
        
        del df_1h, df_15m, df_5m 

# ==============================================================================
# 7. WORKER LIFECYCLE (JITTERED 5M BOUNDARY SCHEDULER)
# ==============================================================================
def get_next_scan_time():
    """Waits for 5-minute boundary + 15 seconds (Jitter) for API Closed Candle Guarantees"""
    now = datetime.utcnow()
    remainder = 5 - (now.minute % 5)
    next_boundary = now + timedelta(minutes=remainder)
    next_boundary = next_boundary.replace(second=15, microsecond=0)
    return next_boundary.timestamp()

def scanner_worker_loop():
    logging.info(f"Prime Samaresh Engine v59.1 Worker Started (ID: {INSTANCE_ID})")
    exchange = None
    while not STOP_EVENT.is_set():
        try:
            next_scan = get_next_scan_time()
            sleep_duration = max(0.0, next_scan - time.time())
            
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
# 8. STREAMLIT UI
# ==============================================================================
st.markdown("## Prime Samaresh Engine v59.1")
st.markdown("**Distributed Institutional Ledger Architecture**")

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
st.metric("Database Status", "Connected 🟢" if DATABASE_URL else "No Database 🔴")
st.metric("Instance ID", INSTANCE_ID)
st.write("---")
st.info("💡 **Note:** Score represents Deterministic Conviction (0-100), not a statistically calibrated win probability.")

st_autorefresh(interval=60000, key="datarefresh")
