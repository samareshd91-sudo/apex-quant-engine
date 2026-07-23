import streamlit as st
import ccxt
import pandas as pd
import numpy as np
import time
import json
import sqlite3
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh

# ================= 👑 1. PAGE CONFIGURATION & LOGGING =================
st.set_page_config(page_title="TRADE MENTOR: APEX QUANT (v36)", layout="wide", initial_sidebar_state="expanded")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SCALPING_COINS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
DB_FILE = "trading_data_v36.db"

# ================= 🎨 2. PREMIUM CSS =================
st.markdown("""
    <style>
    html, body, [data-testid="stAppViewContainer"] { background-color: #0B0E11 !important; color: #EAECEF !important; }
    ::-webkit-scrollbar { width: 8px !important; }
    ::-webkit-scrollbar-thumb { background: #EAECEF !important; border-radius: 10px; }
    .dash-card { background-color: #1E2329; border-radius: 8px; padding: 15px; text-align: center; border-bottom: 3px solid #EAECEF; margin-bottom: 15px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);}
    .stat-card { background-color: #181A20; border-radius: 6px; padding: 10px; text-align: center; border-left: 3px solid #FCD535; margin-bottom: 15px;}
    .stat-title { font-size: 12px; color: #848E9C; text-transform: uppercase; margin-bottom: 5px;}
    .active-trade { background: linear-gradient(90deg, rgba(234,236,239,0.1) 0%, #181A20 100%); border-left: 4px solid #EAECEF; padding: 12px; border-radius: 8px; margin-bottom: 10px; font-size: 14px;}
    .pending-trade { background: linear-gradient(90deg, rgba(255,165,0,0.1) 0%, #181A20 100%); border-left: 4px solid #FFA500; padding: 12px; border-radius: 8px; margin-bottom: 10px; font-size: 14px;}
    div.stButton > button { background-color: #1E2329 !important; color: #EAECEF !important; border: 1px solid #FF1744 !important; margin-top: 10px;}
    div.stButton > button:hover { background-color: #FF1744 !important; color: #FFFFFF !important;}
    </style>
""", unsafe_allow_html=True)

# ================= 🗄️ 3. PURE RELATIONAL SQLITE MANAGER =================
def init_db():
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        # Bot State Table
        c.execute('''CREATE TABLE IF NOT EXISTS bot_state (id INTEGER PRIMARY KEY, balance REAL, total_fees REAL, live_mode INTEGER, cooldowns TEXT, daily_pnl REAL, date_tracker TEXT)''')
        # 🔥 v36 Relational Tables
        c.execute('''CREATE TABLE IF NOT EXISTS open_trades (coin TEXT PRIMARY KEY, data TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS pending_orders (coin TEXT PRIMARY KEY, data TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS trades (uuid TEXT PRIMARY KEY, timestamp INTEGER, time TEXT, coin TEXT, type TEXT, reason TEXT, pnl REAL, fee REAL, risk REAL)''')
        
        c.execute("SELECT id FROM bot_state WHERE id=1")
        if not c.fetchone():
            c.execute("INSERT INTO bot_state (id, balance, total_fees, live_mode, cooldowns, daily_pnl, date_tracker) VALUES (1, 1000.0, 0.0, 0, '{}', 0.0, '')")
        conn.commit()

def load_data():
    init_db()
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        c = conn.cursor()
        c.execute("SELECT balance, total_fees, live_mode, cooldowns, daily_pnl, date_tracker FROM bot_state WHERE id=1")
        row = c.fetchone()
        
        # Load Relational Data
        df_hist = pd.read_sql_query("SELECT * FROM trades ORDER BY timestamp DESC LIMIT 1000", conn)
        st.session_state.trade_history_df = df_hist
        
        c.execute("SELECT coin, data FROM open_trades")
        st.session_state.open_trades = {coin: json.loads(data) for coin, data in c.fetchall()}
        
        c.execute("SELECT coin, data FROM pending_orders")
        st.session_state.pending_orders = {coin: json.loads(data) for coin, data in c.fetchall()}

    current_utc_date = str(datetime.now(timezone.utc).date()) 
    if row:
        st.session_state.balance = row[0]
        st.session_state.total_fees = row[1]
        st.session_state.live_mode = bool(row[2])
        st.session_state.cooldowns = json.loads(row[3]) if row[3] else {}
        if row[5] != current_utc_date:
            st.session_state.daily_pnl = 0.0
            st.session_state.date_tracker = current_utc_date
        else:
            st.session_state.daily_pnl = row[4]
            st.session_state.date_tracker = row[5]

def save_data():
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE") # 🔥 v36 FIX: Transaction locking
        c = conn.cursor()
        c.execute('''UPDATE bot_state SET balance=?, total_fees=?, live_mode=?, cooldowns=?, daily_pnl=?, date_tracker=? WHERE id=1''',
                  (st.session_state.balance, st.session_state.total_fees, int(st.session_state.live_mode),
                   json.dumps(st.session_state.cooldowns), st.session_state.daily_pnl, st.session_state.date_tracker))
        
        # Sync Open Trades
        c.execute("DELETE FROM open_trades")
        for coin, data in st.session_state.open_trades.items():
            c.execute("INSERT INTO open_trades (coin, data) VALUES (?, ?)", (coin, json.dumps(data)))
            
        # Sync Pending Orders
        c.execute("DELETE FROM pending_orders")
        for coin, data in st.session_state.pending_orders.items():
            c.execute("INSERT INTO pending_orders (coin, data) VALUES (?, ?)", (coin, json.dumps(data)))
            
        conn.commit()

def log_trade_db(t_uuid, timestamp, t_time, coin, t_type, reason, pnl, fee, risk):
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE")
        c = conn.cursor()
        c.execute("INSERT INTO trades (uuid, timestamp, time, coin, type, reason, pnl, fee, risk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (str(t_uuid), timestamp, str(t_time), coin, t_type, reason, float(pnl), float(fee), float(risk)))
        conn.commit()
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        st.session_state.trade_history_df = pd.read_sql_query("SELECT * FROM trades ORDER BY timestamp DESC LIMIT 1000", conn)

if 'data_loaded' not in st.session_state:
    load_data()
    st.session_state.data_loaded = True

# ================= 🛡️ RISK ENGINE & UI =================
st.sidebar.markdown("<h3 style='color:#EAECEF;'>⚙️ v36 APEX ENGINE</h3>", unsafe_allow_html=True)

refresh_rate = st.sidebar.slider("রিফ্রেশ রেট (সেকেন্ড)", min_value=3, max_value=30, value=5)
if st.session_state.live_mode:
    st_autorefresh(interval=refresh_rate * 1000, limit=None, key="live_data_refresh")

taker_fee_input = st.sidebar.number_input("Taker Fee (%)", value=0.05, step=0.01) / 100
leverage_input = st.sidebar.number_input("লিভারেজ", value=50, step=10)
max_open_trades = st.sidebar.number_input("ম্যাক্স ট্রেড লিমিট", value=2, min_value=1, max_value=4)
max_daily_loss = st.sidebar.number_input("ডেইলি লস লিমিট", value=100.0, step=10.0)
cooldown_mins = st.sidebar.number_input("কুলডাউন (মিনিট)", value=15, step=5)

STARTING_BALANCE = 1000.0
current_bal = st.session_state.balance
DYNAMIC_RISK_PCT = 0.01 if current_bal >= STARTING_BALANCE else max(0.01 * (current_bal / STARTING_BALANCE), 0.002) 

if st.sidebar.button("🔄 ডাটাবেস রিসেট করুন"):
    with sqlite3.connect(DB_FILE, timeout=30) as conn:
        conn.cursor().execute("DELETE FROM trades")
        conn.commit()
    st.session_state.balance = STARTING_BALANCE
    st.session_state.total_fees = 0.0
    st.session_state.live_mode = False
    st.session_state.open_trades = {}
    st.session_state.pending_orders = {}
    st.session_state.cooldowns = {}
    st.session_state.daily_pnl = 0.0
    st.session_state.date_tracker = str(datetime.now(timezone.utc).date())
    st.session_state.trade_history_df = pd.DataFrame()
    save_data()
    st.rerun()

circuit_breaker_active = st.session_state.daily_pnl <= -max_daily_loss

# ================= ⚡ 4. TRUE WILDER'S INDICATORS & SMC LOGIC =================
@st.cache_data(ttl=max(1, refresh_rate - 1), show_spinner=False) # 🔥 v36 FIX: Streamlit Thread Safety
def fetch_historical_data(symbol, tf, limit):
    local_exch = ccxt.kucoin({'enableRateLimit': True})
    for i in range(3):
        try: return local_exch.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
        except Exception as e:
            if i == 2: logging.error(f"API Error {symbol} ({tf}): {e}")
            time.sleep(1) 
    return None

def fetch_live_ticker(symbol):
    local_exch = ccxt.kucoin({'enableRateLimit': True})
    try: 
        ticker = local_exch.fetch_ticker(symbol)
        return {'bid': ticker['bid'], 'ask': ticker['ask'], 'last': ticker['last']}
    except: return None

def rma(x, n):
    # 🔥 v36 FIX: True Wilder's Smoothing (RMA)
    a = 1 / n
    return x.ewm(alpha=a, adjust=False).mean()

def calculate_indicators(df):
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    
    avg_gain = rma(gain, 14)
    avg_loss = rma(loss, 14)
    rs = avg_gain / (avg_loss + 1e-10)
    df['rsi'] = np.where(avg_loss == 0, 100, 100 - (100 / (1 + rs))) # 🔥 v36 FIX: Perfect TradingView RSI
    
    high, low, prev_close = df['high'], df['low'], df['close'].shift(1)
    tr = pd.Series(np.maximum.reduce([high - low, np.abs(high - prev_close), np.abs(low - prev_close)]), index=df.index)
    df['atr'] = rma(tr, 14)
    
    # 🔥 v36 FIX: Pure Wilder's ADX
    up, down = high.diff(), -low.diff()
    pos_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    neg_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    
    pos_dm_smooth = rma(pos_dm, 14)
    neg_dm_smooth = rma(neg_dm, 14)
    
    df['+di'] = 100 * (pos_dm_smooth / (df['atr'] + 1e-10))
    df['-di'] = 100 * (neg_dm_smooth / (df['atr'] + 1e-10))
    dx = 100 * np.abs(df['+di'] - df['-di']) / (df['+di'] + df['-di'] + 1e-10)
    df['adx'] = rma(dx, 14)
    return df

def analyze_market(coin):
    try:
        current_utc_hour = datetime.now(timezone.utc).hour
        in_session = (7 <= current_utc_hour <= 22)
            
        bars_1h = fetch_historical_data(coin, '1h', 260) 
        if not bars_1h or len(bars_1h) < 220: return None # 🔥 v36 Defensive Check
        df_1h = pd.DataFrame(bars_1h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        ema20, ema50, ema200 = df_1h['close'].ewm(span=20, adjust=False).mean(), df_1h['close'].ewm(span=50, adjust=False).mean(), df_1h['close'].ewm(span=200, adjust=False).mean()
        ema20_slope_up, ema20_slope_down = ema20.iloc[-2] > ema20.iloc[-7], ema20.iloc[-2] < ema20.iloc[-7]
        bias_bullish = (ema20.iloc[-2] > ema50.iloc[-2] > ema200.iloc[-2]) and ema20_slope_up
        bias_bearish = (ema20.iloc[-2] < ema50.iloc[-2] < ema200.iloc[-2]) and ema20_slope_down

        bars_15m = fetch_historical_data(coin, '15m', 100)
        if not bars_15m: return None 
        df_15m = pd.DataFrame(bars_15m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_15m['pivot_high'] = (df_15m['high'] > df_15m['high'].shift(1)) & (df_15m['high'] > df_15m['high'].shift(2)) & (df_15m['high'] > df_15m['high'].shift(-1)) & (df_15m['high'] > df_15m['high'].shift(-2))
        df_15m['pivot_low'] = (df_15m['low'] < df_15m['low'].shift(1)) & (df_15m['low'] < df_15m['low'].shift(2)) & (df_15m['low'] < df_15m['low'].shift(-1)) & (df_15m['low'] < df_15m['low'].shift(-2))
        last_highs, last_lows = df_15m[df_15m['pivot_high']]['high'].dropna().values, df_15m[df_15m['pivot_low']]['low'].dropna().values
        
        struct_bullish = len(last_highs) >= 2 and last_highs[-1] > last_highs[-2]
        struct_bearish = len(last_lows) >= 2 and last_lows[-1] < last_lows[-2]

        bars_5m = fetch_historical_data(coin, '5m', 100)
        if not bars_5m: return None
        df = pd.DataFrame(bars_5m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df = calculate_indicators(df)
        
        atr_val = df['atr'].iloc[-2]
        if atr_val <= 0: return None 
        
        c2, c3, c4 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        closed_price = c2['close']
        
        # 🔥 v36 SMC: Premium/Discount Zone
        swing_high_30, swing_low_30 = df['high'].rolling(30).max().iloc[-2], df['low'].rolling(30).min().iloc[-2]
        equilibrium = (swing_high_30 + swing_low_30) / 2
        in_discount = closed_price < equilibrium
        in_premium = closed_price > equilibrium
        
        ticker = fetch_live_ticker(coin)
        if not ticker: return None
        
        signal = "NORMAL"
        limit_entry_price = ticker['last']
        
        if in_session and not circuit_breaker_active:
            avg_vol = df['volume'].rolling(20).mean().iloc[-2]
            is_vol_spike = c2['volume'] > (avg_vol * 1.5)
            
            bullish_disp, bearish_disp = (c2['close'] - c2['open']) > (atr_val * 0.8), (c2['open'] - c2['close']) > (atr_val * 0.8)
            
            # 🔥 v36 FIX: True CHoCH and BOS Logic
            choch_bullish = c2['close'] > df['high'].iloc[-10:-2].max()
            choch_bearish = c2['close'] < df['low'].iloc[-10:-2].min()
            
            # FVG Check
            has_bullish_fvg = df['low'].iloc[-1] > c3['high']
            has_bearish_fvg = df['high'].iloc[-1] < c3['low']
            
            has_real_bullish_ob = (c3['close'] < c3['open']) and bullish_disp and is_vol_spike and choch_bullish and has_bullish_fvg
            has_real_bearish_ob = (c3['close'] > c3['open']) and bearish_disp and is_vol_spike and choch_bearish and has_bearish_fvg

            mom_bullish, mom_bearish = (df['rsi'].iloc[-2] > 55 and df['adx'].iloc[-2] > 20), (df['rsi'].iloc[-2] < 45 and df['adx'].iloc[-2] > 20)

            if has_real_bullish_ob and struct_bullish and bias_bullish and mom_bullish and in_discount: 
                signal = "BUY"
                limit_entry_price = c3['high'] 
            elif has_real_bearish_ob and struct_bearish and bias_bearish and mom_bearish and in_premium: 
                signal = "SELL"
                limit_entry_price = c3['low'] 

        return {'limit_entry': round(limit_entry_price, 6), 'ticker': ticker, 'candle_high': df['high'].iloc[-1], 'candle_low': df['low'].iloc[-1], 'signal': signal, 'atr': round(atr_val, 6), 'sweep_low': round(swing_low_30, 6), 'sweep_high': round(swing_high_30, 6)}
    except Exception as e:
        logging.error(f"Analysis Error {coin}: {e}")
        return None

all_data = {}
with ThreadPoolExecutor(max_workers=len(SCALPING_COINS)) as executor:
    future_to_coin = {executor.submit(analyze_market, coin): coin for coin in SCALPING_COINS}
    for future in as_completed(future_to_coin):
        coin = future_to_coin[future]
        try: all_data[coin] = future.result()
        except: pass

# ================= 🤖 5. ORDER STATE MACHINE & EXECUTION =================
def place_pending_order(coin, data, trade_type):
    if len(st.session_state.open_trades) + len(st.session_state.pending_orders) >= max_open_trades: return False
    cooldown_secs = cooldown_mins * 60
    if coin in st.session_state.cooldowns and (int(time.time()) - st.session_state.cooldowns[coin]) < cooldown_secs: return False
    
    st.session_state.pending_orders[coin] = {
        'id': str(uuid.uuid4()), 'coin': coin, 'type': trade_type, 
        'limit_price': data['limit_entry'], 'atr': data['atr'],
        'sweep_low': data['sweep_low'], 'sweep_high': data['sweep_high'],
        'time_str': datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
        'timestamp': int(time.time()) 
    }
    return True

def fill_order(coin, pending_order, ticker, leverage):
    trade_type = pending_order['type']
    # 🔥 v36 FIX: Exact Limit Fill Simulation using Bid/Ask
    entry = min(pending_order['limit_price'], ticker['ask']) if trade_type == 'BUY' else max(pending_order['limit_price'], ticker['bid'])
    entry = round(entry, 6)
    
    atr = pending_order['atr']
    sl = min(pending_order['sweep_low'] - (atr * 0.2), entry - (atr * 1.5)) if trade_type == 'BUY' else max(pending_order['sweep_high'] + (atr * 0.2), entry + (atr * 1.5))
    sl = round(sl, 6)
    
    sl_pct = max(abs(entry - sl) / entry, 0.003) # 🔥 v36 FIX: Absolute minimum SL 0.3%
    
    dynamic_risk_inr = st.session_state.balance * DYNAMIC_RISK_PCT
    max_margin_per_trade = st.session_state.balance * 0.15 
    
    ideal_size = dynamic_risk_inr / sl_pct
    avail = st.session_state.balance - sum(t.get('margin', 0) for t in st.session_state.open_trades.values())
    pos_size = min(ideal_size, avail * leverage, max_margin_per_trade * leverage)
    
    margin, pos_size = round(pos_size / leverage, 6), round(pos_size, 6)
    fee = round(pos_size * taker_fee_input, 6)
    
    if avail <= (fee + margin): 
        del st.session_state.pending_orders[coin]; return True 
        
    st.session_state.balance -= fee
    st.session_state.total_fees += fee
    
    st.session_state.open_trades[coin] = {
        'id': pending_order['id'], 'coin': coin, 'type': trade_type, 
        'entry_price': entry, 'sl': sl, 'initial_sl': sl, 
        'highest_price': entry, 'lowest_price': entry,
        'initial_size': pos_size, 'size_inr': pos_size, 'margin': margin, 
        'initial_risk_inr': round(max(pos_size * sl_pct, 0.01), 6),
        'stage': 0, 'realized_pnl': 0.0, 'realized_fee': fee, 
        'time_str': datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    }
    del st.session_state.pending_orders[coin]
    return True

def execute_partial(t, live_p, pct_to_close, new_stage):
    # 🔥 v36 FIX: Exact Remaining Size Calculation
    close_size = round(t['initial_size'] * pct_to_close, 6) if new_stage < 2 else t['size_inr']
    
    gross_pnl = ((live_p - t['entry_price']) / t['entry_price'] * close_size) if t['type'] == 'BUY' else ((t['entry_price'] - live_p) / t['entry_price'] * close_size)
    exit_fee = round(close_size * taker_fee_input, 6)
    net_pnl = round(gross_pnl - exit_fee, 6)
    
    st.session_state.balance += net_pnl
    st.session_state.total_fees += exit_fee
    st.session_state.daily_pnl += net_pnl
    
    t['realized_pnl'] = round(t['realized_pnl'] + net_pnl, 6)
    t['realized_fee'] = round(t['realized_fee'] + exit_fee, 6)
    t['size_inr'] = round(t['size_inr'] - close_size, 6)
    t['margin'] = round(t['size_inr'] / leverage_input, 6)
    t['stage'] = new_stage
    return t

def close_full_trade(coin, live_p, reason):
    t = st.session_state.open_trades[coin]
    close_size = t['size_inr']
    gross_pnl = ((live_p - t['entry_price']) / t['entry_price'] * close_size) if t['type'] == 'BUY' else ((t['entry_price'] - live_p) / t['entry_price'] * close_size)
    exit_fee = round(close_size * taker_fee_input, 6)
    net_pnl = round(gross_pnl - exit_fee, 6)
    
    st.session_state.balance += net_pnl
    st.session_state.total_fees += exit_fee
    st.session_state.daily_pnl += net_pnl
    t['realized_pnl'] = round(t['realized_pnl'] + net_pnl, 6)
    t['realized_fee'] = round(t['realized_fee'] + exit_fee, 6)
    
    log_trade_db(t['id'], int(time.time()), t['time_str'], coin, t['type'], reason, t['realized_pnl'], t['realized_fee'], t['initial_risk_inr'])
    st.session_state.cooldowns[coin] = int(time.time())
    del st.session_state.open_trades[coin]
    return True

# 🔥 v36 Execution Loop & Panic Close Action
def execute_panic_close():
    global_db_dirty = False
    for coin in list(st.session_state.open_trades.keys()):
        ticker = fetch_live_ticker(coin)
        live_p = ticker['bid'] if st.session_state.open_trades[coin]['type'] == 'BUY' else ticker['ask']
        close_full_trade(coin, live_p, 'Circuit Breaker Hit 🛑')
        global_db_dirty = True
    if len(st.session_state.pending_orders) > 0:
        st.session_state.pending_orders.clear()
        global_db_dirty = True
    if global_db_dirty: save_data()

if circuit_breaker_active and len(st.session_state.open_trades) > 0:
    execute_panic_close()
    st.sidebar.error(f"🛑 ইমার্জেন্সি: সার্কিট ব্রেকার হিট করায় সব ট্রেড ক্লোজ করা হয়েছে!")

elif st.session_state.live_mode and not circuit_breaker_active:
    global_db_dirty = False 
    
    # 1. Process Pending Orders 
    for coin, p_order in list(st.session_state.pending_orders.items()):
        if coin in all_data and all_data[coin]:
            d, ticker = all_data[coin], all_data[coin]['ticker']
            limit_p = p_order['limit_price']
            
            # 🔥 v36 FIX: Exact Bid/Ask Trigger
            if (p_order['type'] == 'BUY' and ticker['ask'] <= limit_p) or (p_order['type'] == 'SELL' and ticker['bid'] >= limit_p):
                if fill_order(coin, p_order, ticker, leverage_input): global_db_dirty = True
            elif (int(time.time()) - p_order['timestamp']) > 3600: 
                del st.session_state.pending_orders[coin]; global_db_dirty = True

    # 2. Manage Open Trades
    for coin, t in list(st.session_state.open_trades.items()):
        if coin in all_data and all_data[coin]:
            d, ticker, new_signal = all_data[coin], all_data[coin]['ticker'], all_data[coin]['signal']
            live_p = ticker['bid'] if t['type'] == 'BUY' else ticker['ask'] 
            c_high, c_low = d['candle_high'], d['candle_low']
            risk = abs(t['entry_price'] - t['initial_sl'])
            
            vol_pct = (d['atr'] / live_p) * 100
            trail_mult = 1.2 if vol_pct < 0.3 else (1.8 if vol_pct < 0.8 else 2.8)
            
            if (t['type'] == 'BUY' and new_signal == 'SELL') or (t['type'] == 'SELL' and new_signal == 'BUY'):
                if close_full_trade(coin, live_p, 'Opposite Signal 🔄'): global_db_dirty = True
                continue
            
            if t['type'] == 'BUY':
                if c_high > t['highest_price']: t['highest_price'] = c_high; global_db_dirty = True
                
                if t['stage'] == 0 and live_p >= (t['entry_price'] + risk):
                    t = execute_partial(t, live_p, 0.3, 1)
                    t['sl'] = max(t['sl'], t['entry_price'] * (1 + (taker_fee_input * 2))); st.session_state.open_trades[coin] = t; global_db_dirty = True
                elif t['stage'] == 1 and live_p >= (t['entry_price'] + (risk * 2)):
                    t = execute_partial(t, live_p, 0.3, 2); st.session_state.open_trades[coin] = t; global_db_dirty = True
                
                if t['stage'] > 0:
                    new_sl = max(t['sl'], t['highest_price'] - (d['atr'] * trail_mult))
                    if new_sl > t['sl']: t['sl'] = new_sl; st.session_state.open_trades[coin] = t; global_db_dirty = True
                    
                if live_p <= t['sl']:
                    if close_full_trade(coin, live_p, 'Trailing SL 🛡️' if t['sl'] > t['entry_price'] else 'SL Hit 🛑'): global_db_dirty = True
                    continue
            else: # SELL
                if c_low < t['lowest_price']: t['lowest_price'] = c_low; global_db_dirty = True
                    
                if t['stage'] == 0 and live_p <= (t['entry_price'] - risk):
                    t = execute_partial(t, live_p, 0.3, 1)
                    t['sl'] = min(t['sl'], t['entry_price'] * (1 - (taker_fee_input * 2))); st.session_state.open_trades[coin] = t; global_db_dirty = True
                elif t['stage'] == 1 and live_p <= (t['entry_price'] - (risk * 2)):
                    t = execute_partial(t, live_p, 0.3, 2); st.session_state.open_trades[coin] = t; global_db_dirty = True
                
                if t['stage'] > 0:
                    new_sl = min(t['sl'], t['lowest_price'] + (d['atr'] * trail_mult))
                    if new_sl < t['sl']: t['sl'] = new_sl; st.session_state.open_trades[coin] = t; global_db_dirty = True
                
                if live_p >= t['sl']:
                    if close_full_trade(coin, live_p, 'Trailing SL 🛡️' if t['sl'] < t['entry_price'] else 'SL Hit 🛑'): global_db_dirty = True
                    continue
                    
    # 3. Process New Signals
    for coin, data in all_data.items():
        if data and data['signal'] in ["BUY", "SELL"] and coin not in st.session_state.open_trades and coin not in st.session_state.pending_orders:
            if place_pending_order(coin, data, data['signal']): global_db_dirty = True
            
    if global_db_dirty: save_data() 

# ================= 📊 6. QUANT ANALYTICS DASHBOARD =================
st.markdown("<h3 style='color:#EAECEF;'>📊 2026 কোয়ান্ট ড্যাশবোর্ড (v36)</h3>", unsafe_allow_html=True)
tc1, tc2 = st.columns([1, 4])
with tc1: st.toggle("🔴 অটো ট্রেডিং অন/অফ", key='live_mode', on_change=save_data)

df_hist = st.session_state.trade_history_df
total_trades = len(df_hist)
wins = len(df_hist[df_hist['pnl'] > 0]) if total_trades > 0 else 0
win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0.0

gross_profit = df_hist[df_hist['pnl'] > 0]['pnl'].sum() if total_trades > 0 else 0
gross_loss = abs(df_hist[df_hist['pnl'] <= 0]['pnl'].sum()) if total_trades > 0 else 0
profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float('nan') if gross_loss == 0 and gross_profit == 0 else float('inf')) 

max_dd, expectancy = 0.0, 0.0
if not df_hist.empty:
    # 🔥 v36 FIX: Exact Equity Drawdown
    equity_curve = 1000.0 + df_hist['pnl'].cumsum()
    max_dd = (equity_curve.cummax() - equity_curve).max()
    
    df_hist['risk'] = df_hist['risk'].clip(lower=0.01)
    df_hist['R_Multiple'] = df_hist['pnl'] / df_hist['risk']
    expectancy = df_hist['R_Multiple'].mean()

total_profit = st.session_state.balance - 1000.0

c1, c2, c3, c4 = st.columns(4)
with c1: st.markdown(f"<div class='dash-card'><div class='stat-title'>অ্যাভেইলেবল ব্যালেন্স</div><b style='font-size:22px;'>₹{st.session_state.balance:.2f}</b></div>", unsafe_allow_html=True)
with c2: st.markdown(f"<div class='dash-card'><div class='stat-title'>নিট লাভ/ক্ষতি</div><b style='font-size:22px; color:{'#00FF00' if total_profit>=0 else '#FF1744'};'>₹{total_profit:.2f}</b></div>", unsafe_allow_html=True)
with c3: st.markdown(f"<div class='dash-card'><div class='stat-title'>ডেইলি PnL (UTC)</div><b style='font-size:22px; color:{'#00FF00' if st.session_state.daily_pnl>=0 else '#FF1744'};'>₹{st.session_state.daily_pnl:.2f}</b></div>", unsafe_allow_html=True)
with c4: st.markdown(f"<div class='dash-card'><div class='stat-title'>Max Drawdown</div><b style='font-size:22px; color:#FF1744;'>₹{max_dd:.2f}</b></div>", unsafe_allow_html=True)

s1, s2, s3, s4 = st.columns(4)
with s1: st.markdown(f"<div class='stat-card'><div class='stat-title'>উইন রেট</div><b style='font-size:18px; color:#00FF00;'>{win_rate:.1f}%</b></div>", unsafe_allow_html=True)
with s2: st.markdown(f"<div class='stat-card'><div class='stat-title'>প্রফিট ফ্যাক্টর</div><b style='font-size:18px; color:#FCD535;'>{'∞' if profit_factor == float('inf') else f'{profit_factor:.2f}'}</b></div>", unsafe_allow_html=True)
with s3: st.markdown(f"<div class='stat-card'><div class='stat-title'>Expectancy (R)</div><b style='font-size:18px; color:#00FFCC;'>{expectancy:.2f} R</b></div>", unsafe_allow_html=True)
with s4: st.markdown(f"<div class='stat-card'><div class='stat-title'>মোট ট্রেড</div><b style='font-size:18px;'>{total_trades}</b></div>", unsafe_allow_html=True)

# ================= 🟢 7. ACTIVE & PENDING TRADES =================
st.markdown("<h4>⚡ পেন্ডিং ও চলমান ট্রেডসমূহ</h4>", unsafe_allow_html=True)

if len(st.session_state.pending_orders) > 0:
    for coin, p in st.session_state.pending_orders.items():
        st.markdown(f"<div class='pending-trade'><b>{coin} ({p['type']})</b> | 🕒 PENDING (Limit Order)<br>📍 লিমিট প্রাইস: {p['limit_price']:.4f} | ⏳ Time: {p['time_str']}</div>", unsafe_allow_html=True)

if len(st.session_state.open_trades) > 0:
    for coin, t in st.session_state.open_trades.items():
        ticker = all_data.get(coin, {}).get('ticker', {}) if coin in all_data else {}
        live_p = ticker.get('bid', t['entry_price']) if t['type'] == 'BUY' else ticker.get('ask', t['entry_price'])
        
        live_pnl = (((live_p - t['entry_price']) / t['entry_price']) * t['size_inr']) if t['type'] == 'BUY' else (((t['entry_price'] - live_p) / t['entry_price']) * t['size_inr'])
        total_running_pnl = t['realized_pnl'] + live_pnl
        lp_color = "#00FF00" if total_running_pnl > 0 else "#FF1744"
        stages = ["⏳ Wait for 1R", "🚀 30% Booked", "🔥 60% Booked (Runner)"]
        
        html = (f"<div class='active-trade'>"
                f"<b>{coin} ({t['type']})</b> | {stages[t['stage']]} <br>"
                f"📍 এন্ট্রি (Filled): {t['entry_price']:.4f} | ⚡ লাইভ (Bid/Ask): {live_p:.4f} | 🛑 SL: {t['sl']:.4f}<br>"
                f"💼 <b>রানিং সাইজ:</b> ₹{t['size_inr']:.2f} | 💸 Total PnL: <b style='color:{lp_color};'>₹{total_running_pnl:.2f}</b>"
                f"</div>")
        st.markdown(html, unsafe_allow_html=True)

if len(st.session_state.pending_orders) == 0 and len(st.session_state.open_trades) == 0:
    st.info("এই মুহূর্তে কোনো ওপেন বা পেন্ডিং ট্রেড নেই। বট সিগন্যালের জন্য অপেক্ষা করছে...")

st.markdown("<h4>📜 ট্রেড হিস্টোরি</h4>", unsafe_allow_html=True)
if not df_hist.empty: 
    display_df = df_hist.drop(columns=['risk', 'R_Multiple', 'timestamp'], errors='ignore') 
    st.dataframe(display_df, use_container_width=True, hide_index=True)
