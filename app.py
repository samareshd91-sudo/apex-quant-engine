import streamlit as st
import ccxt
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from streamlit_autorefresh import st_autorefresh
import logging
import os
import time
import json
import threading
import gc
import atexit
import psutil

# ================= ⚙️ V56.6 LIVE CONFIGURATION =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

SIGNAL_FILE = "sent_signals.json" 

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    try:
        TELEGRAM_BOT_TOKEN = st.secrets["telegram"]["bot_token"]
        TELEGRAM_CHAT_ID = st.secrets["telegram"]["chat_id"]
    except Exception:
        logging.warning("Telegram credentials missing. Alerts will not be sent.")

COINS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
EXECUTION_THRESHOLD = 75  
TARGET_RR = 2.4
DATA_LIMIT = 100 

telegram_session = requests.Session()
atexit.register(lambda: telegram_session.close())

st.set_page_config(page_title="Prime Samaresh Live Terminal v56.6", layout="wide")
st_autorefresh(interval=60000, key="v56_live_refresh")

st.markdown("""
    <style>
    .signal-buy { background-color: rgba(0,255,170,0.15); padding: 12px; border-radius: 8px; border-left: 6px solid #00FFAA; }
    .signal-sell { background-color: rgba(255,68,68,0.15); padding: 12px; border-radius: 8px; border-left: 6px solid #FF4444; }
    .signal-wait { background-color: rgba(255,255,255,0.03); padding: 10px; border-radius: 8px; border-left: 6px solid #666; }
    </style>
""", unsafe_allow_html=True)

# ================= 🧹 MEMORY CLEANER =================
def clean_memory():
    try:
        gc.collect()
        logging.info("Memory cleanup completed.")
    except Exception as e:
        logging.warning(f"Memory cleanup failed: {e}")

# ================= 🛡️ RENDER SYSTEM GUARDIAN =================
def render_guardian():
    try:
        process = psutil.Process()
        rss_memory_mb = process.memory_info().rss / (1024 * 1024)
        # 🟢 Meaningful CPU reading with slight interval
        cpu_percent = process.cpu_percent(interval=0.1)
        
        logging.info(f"App Process Memory (RSS): {rss_memory_mb:.2f} MB | CPU: {cpu_percent}%")
        
        if rss_memory_mb > 400:
            logging.warning(f"High App Memory detected ({rss_memory_mb:.2f} MB)! Running GC...")
            clean_memory()
            
    except Exception as e:
        logging.warning(f"Guardian Check Failed: {e}")

# ================= 📡 SAFE API FETCHING & CACHING =================
@st.cache_resource(ttl=3600)
def get_exchange():
    return ccxt.kucoin({'enableRateLimit': True, 'timeout': 15000})

def safe_fetch_ohlcv(symbol, timeframe, limit):
    exchange = get_exchange()
    for attempt in range(2):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            if ohlcv:
                df = pd.DataFrame(ohlcv, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                return df
        except ccxt.RateLimitExceeded:
            time.sleep(2)
        except ccxt.NetworkError:
            time.sleep(1)
        except Exception as e:
            logging.error(f"Unexpected Error {symbol} ({timeframe}): {e}")
            break
    return None

@st.cache_data(ttl=600, max_entries=10, show_spinner=False)
def get_1h_data(symbol):
    return safe_fetch_ohlcv(symbol, '1h', DATA_LIMIT)

@st.cache_data(ttl=300, max_entries=10, show_spinner=False)
def get_15m_data(symbol):
    return safe_fetch_ohlcv(symbol, '15m', DATA_LIMIT)

@st.cache_data(ttl=55, max_entries=10, show_spinner=False)
def get_5m_data(symbol):
    return safe_fetch_ohlcv(symbol, '5m', DATA_LIMIT)

# ================= 🧮 2026 INSTITUTIONAL SCORING ENGINE =================
def rma(x, n):
    return x.ewm(alpha=1/n, adjust=False).mean()

def process_market_data(coin):
    try:
        raw_1h = get_1h_data(coin)
        raw_15m = get_15m_data(coin)
        raw_5m = get_5m_data(coin)
        
        if raw_1h is None or raw_15m is None or raw_5m is None:
            return None
            
        df_1h = raw_1h.tail(DATA_LIMIT).copy()
        df_15m = raw_15m.tail(DATA_LIMIT).copy()
        df = raw_5m.tail(DATA_LIMIT).copy()
        
        df_1h['swing_high_20'] = df_1h['high'].rolling(20).max()
        df_1h['swing_low_20'] = df_1h['low'].rolling(20).min()
        df_1h['1h_bull_struct'] = df_1h['close'] > df_1h['swing_high_20'].shift(1)
        df_1h['1h_bear_struct'] = df_1h['close'] < df_1h['swing_low_20'].shift(1)
        
        df_15m['ema50_15m'] = df_15m['close'].ewm(span=50).mean()
        df_15m['15m_bull'] = df_15m['close'] > df_15m['ema50_15m']
        df_15m['15m_bear'] = df_15m['close'] < df_15m['ema50_15m']
        
        df = pd.merge_asof(df, df_15m[['datetime', '15m_bull', '15m_bear']], on='datetime', direction='backward')
        df = pd.merge_asof(df, df_1h[['datetime', '1h_bull_struct', '1h_bear_struct']], on='datetime', direction='backward')
        
        tr = pd.concat([
            df['high'] - df['low'],
            (df['high'] - df['close'].shift()).abs(),
            (df['low'] - df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        df['atr'] = rma(tr, 14)
        
        df['atr_pct'] = df['atr'].rank(pct=True)
        
        df['bb_width'] = (df['high'].rolling(20).max() - df['low'].rolling(20).min()) / df['close']
        df['regime_trend'] = (df['bb_width'].shift(1) > df['bb_width'].rolling(50).mean().shift(1))
        
        df['eqh'] = abs(df['high'] - df['high'].shift(1)) < (df['atr'] * 0.1)
        df['eql'] = abs(df['low'] - df['low'].shift(1)) < (df['atr'] * 0.1)
        df['swing_high_10'] = df['high'].rolling(10).max().shift(1)
        df['swing_low_10'] = df['low'].rolling(10).min().shift(1)
        df['candle_delta'] = np.where(df['close'] > df['open'], df['volume'], -df['volume'])
        df['delta_ema'] = df['candle_delta'].ewm(span=10).mean()
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['datetime_ist'] = df['datetime'] + pd.Timedelta(hours=5, minutes=30)
        
        return df
    except Exception as e:
        logging.error(f"Processing Error for {coin}: {e}")
        return None

def generate_signals(all_data):
    signals = []
    btc_df = all_data.get('BTC/USDT')
    btc_bull, btc_bear = False, False
    if btc_df is not None:
        btc_bull = btc_df['close'].iloc[-2] > btc_df['ema50'].iloc[-2]
        btc_bear = btc_df['close'].iloc[-2] < btc_df['ema50'].iloc[-2]

    for coin, df in all_data.items():
        if df is None or len(df) < 5: 
            signals.append({'coin': coin, 'direction': 'ERROR', 'price': 0, 'score': 0})
            continue
        
        c2 = df.iloc[-2] 
        c4 = df.iloc[-4]
        live_candle = df.iloc[-1] 
        
        bull_liquidity_grab = (c2['low'] < c2['swing_low_10']) and df['eql'].iloc[-4]
        bear_liquidity_grab = (c2['high'] > c2['swing_high_10']) and df['eqh'].iloc[-4]
        has_bull_fvg = (c2['low'] > c4['high']) and ((c2['low'] - c4['high']) > (c2['atr'] * 0.15))
        has_bear_fvg = (c4['low'] > c2['high']) and ((c4['low'] - c2['high']) > (c2['atr'] * 0.15))
        
        bull_orderflow = c2['delta_ema'] > 0
        bear_orderflow = c2['delta_ema'] < 0
        
        hour = c2['datetime_ist'].hour
        aggressive_session = hour in [12, 13, 14, 18, 19, 20]
        good_volatility = 0.40 <= c2['atr_pct'] <= 0.85
        
        wick_ratio_bull = (c2['high'] - c2['close']) / (c2['high'] - c2['low'] + 1e-10)
        wick_ratio_bear = (c2['close'] - c2['low']) / (c2['high'] - c2['low'] + 1e-10)
        mm_trap_bull = (c2['low'] < c2['swing_low_10']) and (wick_ratio_bull > 0.6) and (c2['close'] > c2['open'])
        mm_trap_bear = (c2['high'] > c2['swing_high_10']) and (wick_ratio_bear > 0.6) and (c2['close'] < c2['open'])

        bull_score, bear_score = 0, 0
        
        if c2.get('1h_bull_struct', False): bull_score += 20
        if c2.get('15m_bull', False): bull_score += 15
        if bull_liquidity_grab: bull_score += 15
        if has_bull_fvg: bull_score += 15
        if bull_orderflow: bull_score += 15
        if aggressive_session: bull_score += 10
        if good_volatility and c2['regime_trend']: bull_score += 10
        if mm_trap_bull: bull_score += 15
        if coin != 'BTC/USDT' and btc_bull: bull_score += 10
        
        if c2.get('1h_bear_struct', False): bear_score += 20
        if c2.get('15m_bear', False): bear_score += 15
        if bear_liquidity_grab: bear_score += 15
        if has_bear_fvg: bear_score += 15
        if bear_orderflow: bear_score += 15
        if aggressive_session: bear_score += 10
        if good_volatility and c2['regime_trend']: bear_score += 10
        if mm_trap_bear: bear_score += 15
        if coin != 'BTC/USDT' and btc_bear: bear_score += 10

        exec_score = min(max(bull_score, bear_score), 100)
        direction = "WAIT"
        confidence = "LOW"
        entry, sl, tp = 0.0, 0.0, 0.0
        
        raw_direction = "BUY" if bull_score > bear_score else "SELL"
        
        # 🟢 Intrabar Whipsaw Protection: Confirmation taken from closed candle (c2)
        candle_confirmed = False
        if raw_direction == "BUY" and c2['close'] > c2['open']:
            candle_confirmed = True
        elif raw_direction == "SELL" and c2['close'] < c2['open']:
            candle_confirmed = True

        if exec_score >= EXECUTION_THRESHOLD and candle_confirmed:
            direction = raw_direction
            confidence = "HIGH" if exec_score >= 85 else "MEDIUM"
            
            sl_dist = c2['atr'] * 1.5
            variable_slippage = c2['atr'] * 0.03
            
            if direction == "BUY":
                entry = live_candle['close'] + variable_slippage
                sl = entry - sl_dist
                tp = entry + (sl_dist * TARGET_RR)
            else:
                entry = live_candle['close'] - variable_slippage
                sl = entry + sl_dist
                tp = entry - (sl_dist * TARGET_RR)

        signals.append({
            'coin': coin, 'direction': direction, 'score': exec_score, 'confidence': confidence,
            'entry': round(entry, 4) if entry > 0 else "-", 'sl': round(sl, 4) if sl > 0 else "-",
            'tp': round(tp, 4) if tp > 0 else "-", 'time': c2['datetime_ist'].strftime('%Y-%m-%d %H:%M:%S'),
            'price': round(live_candle['close'], 4)
        })
    return signals

# ================= 🚀 SAFE TELEGRAM DELIVERY =================
file_lock = threading.Lock() 

def load_sent_signals():
    with file_lock:
        if os.path.exists(SIGNAL_FILE):
            try:
                with open(SIGNAL_FILE, "r") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

def save_sent_signal(coin, signal_key):
    with file_lock:
        signals = {}
        if os.path.exists(SIGNAL_FILE):
            try:
                with open(SIGNAL_FILE, "r") as f:
                    signals = json.load(f)
            except Exception:
                pass
        
        signals[coin] = signal_key
        
        if len(signals) > 50:
            signals = dict(list(signals.items())[-50:])
            
        try:
            temp_file = SIGNAL_FILE + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(signals, f)
            os.replace(temp_file, SIGNAL_FILE)
        except Exception as e:
            logging.error(f"Failed to save signal locally: {e}")

def send_telegram_alert(sig):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
        
    coin = sig['coin']
    signal_key = f"{coin}_{sig['direction']}_{sig['time']}"
    
    saved_signals = load_sent_signals()
    
    if saved_signals.get(coin) != signal_key:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        emoji = "🟢" if sig['direction'] == "BUY" else "🔴"
        # 🟢 Terminology updated to Institutional Score
        msg = f"⚡ *V56.6 INSTITUTIONAL ALERT*\n\n🪙 *Asset:* {sig['coin']}\n{emoji} *Action:* {sig['direction']}\n🎯 *Inst. Score:* {sig['score']}/100 ({sig['confidence']})\n\n💵 *Entry:* {sig['entry']}\n🛑 *SL:* {sig['sl']}\n🚀 *TP:* {sig['tp']}\n⏱ *Time:* {sig['time']}"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
        
        try:
            response = telegram_session.post(url, data=payload, timeout=8)
            if response.status_code == 200:
                save_sent_signal(coin, signal_key)
            else:
                logging.warning(f"Telegram Failed (HTTP {response.status_code}): {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Telegram Network Error: {e}")

# ================= 🖥️ STREAMLIT UI =================
st.title("⚡ Prime Samaresh Live Terminal (v56.6)")
st.caption("24/7 Conservative Scanner | Whipsaw Protected | Safe Guardian Active")
st.markdown("---")

col1, col2, col3 = st.columns(3)
col1.metric("Mode", "Live Operational", "60s Safe Refresh")
# 🟢 Terminology updated to Signal Score
col2.metric("Threshold", f"{EXECUTION_THRESHOLD}+ Signal Score", "Strict Filter Active")
col3.metric("Status", "Connected", "KuCoin API (TTL Cached)")

with st.spinner("Scanning live market orderflow & strict confirmation rules..."):
    all_live_data = {}
    for coin in COINS:
        all_live_data[coin] = process_market_data(coin)
        
    live_signals = generate_signals(all_live_data)
    
    st.subheader("📡 Validated Live Signals")
    
    for sig in live_signals:
        if sig['direction'] == "ERROR":
             st.markdown(f"""
            <div class="signal-wait" style="border-left: 6px solid #FFaa00;">
                <h4>⚠️ {sig['coin']} - API UNAVAILABLE</h4>
                <small>KuCoin Data fetch failed for this asset. Will retry next cycle.</small>
            </div><br>
            """, unsafe_allow_html=True)
             
        elif sig['direction'] == "BUY":
            st.markdown(f"""
            <div class="signal-buy">
                <h3>🟢 {sig['coin']} - BUY SIGNAL</h3>
                <b>Inst. Score:</b> {sig['score']}/100 ({sig['confidence']} Confidence)<br>
                <b>Entry:</b> {sig['entry']} | <b>SL:</b> {sig['sl']} | <b>TP:</b> {sig['tp']} @ 2.4R<br>
                <small>Candle Time: {sig['time']} (IST)</small>
            </div><br>
            """, unsafe_allow_html=True)
            send_telegram_alert(sig)
            
        elif sig['direction'] == "SELL":
            st.markdown(f"""
            <div class="signal-sell">
                <h3>🔴 {sig['coin']} - SELL SIGNAL</h3>
                <b>Inst. Score:</b> {sig['score']}/100 ({sig['confidence']} Confidence)<br>
                <b>Entry:</b> {sig['entry']} | <b>SL:</b> {sig['sl']} | <b>TP:</b> {sig['tp']} @ 2.4R<br>
                <small>Candle Time: {sig['time']} (IST)</small>
            </div><br>
            """, unsafe_allow_html=True)
            send_telegram_alert(sig)
            
        else:
            st.markdown(f"""
            <div class="signal-wait">
                <h4>⚪ {sig['coin']} - WAIT</h4>
                <b>Inst. Score:</b> {sig['score']}/100 | Market Price: {sig['price']}
            </div><br>
            """, unsafe_allow_html=True)

st.markdown("---")
st.success("✅ **v56.6 Status:** Closed-Candle Logic Enabled, CPU Interval Set, Institutional Scoring Active.")

# ================= 🧹 AUTO MEMORY RELEASE & GUARDIAN TRIGGER =================
try:
    del all_live_data
    del live_signals
except Exception:
    pass

if "cleanup_counter" not in st.session_state:
    st.session_state.cleanup_counter = 0

st.session_state.cleanup_counter += 1

if st.session_state.cleanup_counter >= 30:
    clean_memory()
    st.session_state.cleanup_counter = 0

render_guardian()
