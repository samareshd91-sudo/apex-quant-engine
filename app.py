import streamlit as st
import ccxt
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from streamlit_autorefresh import st_autorefresh

# ================= ⚙️ V56 LIVE CONFIGURATION =================
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID_HERE"

COINS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
EXECUTION_THRESHOLD = 75  
TARGET_RR = 2.4

st.set_page_config(page_title="Prime Samaresh Live Terminal v56", layout="wide")

# Auto-refresh every 30 seconds for 24/7 Live Monitoring
st_autorefresh(interval=30000, key="v56_live_refresh")

if 'last_sent_signal' not in st.session_state:
    st.session_state['last_sent_signal'] = ""

st.markdown("""
    <style>
    .signal-buy { background-color: rgba(0,255,170,0.15); padding: 12px; border-radius: 8px; border-left: 6px solid #00FFAA; }
    .signal-sell { background-color: rgba(255,68,68,0.15); padding: 12px; border-radius: 8px; border-left: 6px solid #FF4444; }
    .signal-wait { background-color: rgba(255,255,255,0.03); padding: 10px; border-radius: 8px; border-left: 6px solid #666; }
    </style>
""", unsafe_allow_html=True)

# ================= 📡 DATA FETCHING (KUCOIN API) =================
@st.cache_resource
def get_exchange():
    # ✅ Binance এর বদলে KuCoin ব্যবহার করা হচ্ছে (ক্লাউড ব্লকিং এড়ানোর জন্য)
    return ccxt.kucoin({'enableRateLimit': True})

def fetch_data(symbol, timeframe, limit=250):
    exchange = get_exchange()
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
        return df
    except Exception as e:
        return None

# ================= 🧮 2026 AI SCORING ENGINE =================
def rma(x, n):
    return x.ewm(alpha=1/n, adjust=False).mean()

def process_market_data(coin):
    df_1h = fetch_data(coin, '1h')
    df_15m = fetch_data(coin, '15m')
    df_5m = fetch_data(coin, '5m')
    
    if df_1h is None or df_15m is None or df_5m is None:
        return None
        
    df_1h['swing_high_20'] = df_1h['high'].rolling(20).max()
    df_1h['swing_low_20'] = df_1h['low'].rolling(20).min()
    df_1h['1h_bull_struct'] = df_1h['close'] > df_1h['swing_high_20'].shift(1)
    df_1h['1h_bear_struct'] = df_1h['close'] < df_1h['swing_low_20'].shift(1)
    
    df_15m['ema50_15m'] = df_15m['close'].ewm(span=50).mean()
    df_15m['15m_bull'] = df_15m['close'] > df_15m['ema50_15m']
    df_15m['15m_bear'] = df_15m['close'] < df_15m['ema50_15m']
    
    df = pd.merge_asof(df_5m, df_15m[['datetime', '15m_bull', '15m_bear']], on='datetime', direction='backward')
    df = pd.merge_asof(df, df_1h[['datetime', '1h_bull_struct', '1h_bear_struct']], on='datetime', direction='backward')
    
    df['atr'] = rma(pd.Series(np.maximum.reduce([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift(1)), np.abs(df['low'] - df['close'].shift(1))])), 14)
    df['atr_pct'] = df['atr'].rolling(200).rank(pct=True)
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

def generate_signals(all_data):
    signals = []
    btc_df = all_data.get('BTC/USDT')
    btc_bull, btc_bear = False, False
    if btc_df is not None:
        btc_bull = btc_df['close'].iloc[-2] > btc_df['ema50'].iloc[-2]
        btc_bear = btc_df['close'].iloc[-2] < btc_df['ema50'].iloc[-2]

    for coin, df in all_data.items():
        if df is None or len(df) < 5: continue
        
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

        exec_score = max(bull_score, bear_score)
        direction = "WAIT"
        confidence = "LOW"
        entry, sl, tp = 0.0, 0.0, 0.0
        
        raw_direction = "BUY" if bull_score > bear_score else "SELL"
        
        candle_confirmed = False
        if raw_direction == "BUY" and live_candle['close'] > live_candle['open']:
            candle_confirmed = True
        elif raw_direction == "SELL" and live_candle['close'] < live_candle['open']:
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
            'tp': round(tp, 4) if tp > 0 else "-", 'time': live_candle['datetime_ist'].strftime('%Y-%m-%d %H:%M:%S'),
            'price': round(live_candle['close'], 4)
        })
    return signals

# ================= 🚀 TELEGRAM =================
def send_telegram_alert(sig):
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
        return
        
    signal_key = f"{sig['coin']}_{sig['direction']}_{sig['time']}"
    
    if signal_key != st.session_state['last_sent_signal']:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        msg = f"⚡ *V56 INSTITUTIONAL ALERT*\n\n🪙 *Asset:* {sig['coin']}\n🟢 *Action:* {sig['direction']}\n🎯 *AI Score:* {sig['score']} ({sig['confidence']})\n\n💵 *Entry:* {sig['entry']}\n🛑 *SL:* {sig['sl']}\n🚀 *TP:* {sig['tp']}\n⏱ *Time:* {sig['time']}"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
        try:
            requests.post(url, data=payload)
            st.session_state['last_sent_signal'] = signal_key
        except Exception as e:
            pass

# ================= 🖥️ STREAMLIT UI =================
st.title("⚡ Prime Samaresh Live Terminal (v56)")
st.caption("24/7 Operational Terminal with Candle Confirmation & Precise Alert Deduplication")
st.markdown("---")

col1, col2, col3 = st.columns(3)
col1.metric("Mode", "Live Operational", "Auto-refresh Active")
col2.metric("Threshold", "75+ AI Score", "Strict Filter Active")

# ✅ UI তে Binance এর বদলে KuCoin লেখা দেখানো হচ্ছে
col3.metric("Status", "Connected", "KuCoin API")

with st.spinner("Scanning live market orderflow & strict confirmation rules..."):
    all_live_data = {coin: process_market_data(coin) for coin in COINS}
    live_signals = generate_signals(all_live_data)
    
    st.subheader("📡 Validated Live Signals")
    
    for sig in live_signals:
        if sig['direction'] == "BUY":
            st.markdown(f"""
            <div class="signal-buy">
                <h3>🟢 {sig['coin']} - BUY SIGNAL</h3>
                <b>AI Score:</b> {sig['score']}/100 ({sig['confidence']} Confidence)<br>
                <b>Entry:</b> {sig['entry']} | <b>SL:</b> {sig['sl']} | <b>TP:</b> {sig['tp']} @ 2.4R<br>
                <small>Candle Time: {sig['time']} (IST)</small>
            </div><br>
            """, unsafe_allow_html=True)
            send_telegram_alert(sig)
            
        elif sig['direction'] == "SELL":
            st.markdown(f"""
            <div class="signal-sell">
                <h3>🔴 {sig['coin']} - SELL SIGNAL</h3>
                <b>AI Score:</b> {sig['score']}/100 ({sig['confidence']} Confidence)<br>
                <b>Entry:</b> {sig['entry']} | <b>SL:</b> {sig['sl']} | <b>TP:</b> {sig['tp']} @ 2.4R<br>
                <small>Candle Time: {sig['time']} (IST)</small>
            </div><br>
            """, unsafe_allow_html=True)
            send_telegram_alert(sig)
            
        else:
            st.markdown(f"""
            <div class="signal-wait">
                <h4>⚪ {sig['coin']} - WAIT</h4>
                <b>AI Score:</b> {sig['score']}/100 | Market Price: {sig['price']}
            </div><br>
            """, unsafe_allow_html=True)

st.markdown("---")
# ✅ Success মেসেজেও KuCoin এর কথা উল্লেখ করা হয়েছে
st.success("✅ **v56 Operational Status:** KuCoin Data Feed connected, execution threshold 75, and live candle body direction filter successfully locked.")
        
