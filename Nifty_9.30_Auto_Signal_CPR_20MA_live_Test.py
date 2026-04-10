# ==========================================================
# ULTRA-PRO OPTION BUYING – LIVE TRADE VERSION
# (Same Framework – LIVE ORDER EXECUTION ADDED)
# ==========================================================

# ================= CONFIG =================
CLIENT_ID ="1106176565"
ACCESS_TOKEN ="927X2ijccpAleBJKsAFvIe9OXBvpMSoS"
DHAN_CLIENT_ID = "YOUR_CLIENT_ID"
DHAN_ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"
# ==========================================================



from dhanhq import DhanContext, dhanhq, MarketFeed
from datetime import datetime, date, time as dtime, timedelta
import time, threading, sys

try:
    import winsound
except:
    winsound = None


from colorama import init
import logging

logging.getLogger("websocket").setLevel(logging.CRITICAL)
logging.getLogger("kiteconnect").setLevel(logging.CRITICAL)

init(autoreset=True)

GREEN="\033[92m"; RED="\033[91m"; YELLOW="\033[93m"
BLUE="\033[94m"; RESET="\033[0m"

#lock
import atexit
import os

# this is for cloud
#LOCK_FILE = "/tmp/trading.lock"

# this is for local PC
LOCK_FILE = "trading.lock"

def remove_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

atexit.register(remove_lock)

if os.path.exists(LOCK_FILE):
    print("Script already running — exiting")
    exit(0)

open(LOCK_FILE, "w").close()


#Telegram

import requests

BOT_TOKEN = "8565948222:AAHym1kW4PCTMVAcPvZNLpKjzpsbdDWryjg"
CHAT_ID = 1412356698

def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": f"<pre>{msg}</pre>",
            "parse_mode": "HTML"
        }
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print("Telegram Error:", e)

# ================= CONFIG =================
MODE="LIVE"
PRODUCT="INTRADAY"
EXCHANGE="NSE_FNO"
ORDER_TYPE="MARKET"

SPOT_TOKEN="13"
LOT_SIZE=130

PREM_SL_PTS=20
PREM_TGT_PTS=40	

LAST_ENTRY_TIME=dtime(15,15)
FORCE_EXIT_TIME=dtime(15,20)

CPR_WIDE_THRESHOLD=0.6

# ================= GLOBALS =================
spot_ltp=None
option_ltp=None
trade_open=False
ACTIVE_OPTION_TOKEN=None
ACTIVE_SYMBOL=None
ACTIVE_SIDE=None
ORDER_PLACED=False
BLOCK_MSG_SHOWN=False
day_closed = False
SCRIPT_RUNNING = True
WS_STOPPED = False
LAST_OPTION_TICK_TS = 0.0
LAST_SPOT_TICK_TS = 0.0
LAST_ANY_TICK_TS = 0.0

AUTO_SIGNAL="NO TRADE"
EXECUTION_MODE = "DHAN_PRIMARY"
LAST_BROKER_USED = "DHAN"
allowed_side=None
MA_SIDE=None
CPR_TYPE=None
AUTO_READY = False

trade={}
day_closed=False
PENDING_ENTRY_SYMBOL = None
PENDING_ENTRY_FILL = False
OPEN_QTY_CACHE = {"symbol": None, "qty": 0, "ts": 0.0}
OPEN_QTY_CACHE_TTL = 2.0
PRE_CE_TOKEN = None
PRE_PE_TOKEN = None
PRE_CE_SYMBOL = None
PRE_PE_SYMBOL = None

candle={"high":None,"low":None}
candle_done=False

# ================= DHAN =================
# BROKER CHANGE: Kite client replaced with Dhan client/context
_dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan = dhanhq(_dhan_context)


def _to_float(v):
    try:
        return float(v)
    except:
        return None


def _extract_data(resp):
    if isinstance(resp, dict):
        return resp.get("data", resp.get("Data", resp))
    return resp


def _normalize_orders(raw):
    data = _extract_data(raw)
    if isinstance(data, list):
        return data
    return []


def _normalize_positions(raw):
    data = _extract_data(raw)
    if isinstance(data, list):
        return data
    return []


def _get_ltp_by_security_id(security_id, exchange_segment):
    # BROKER CHANGE: Dhan LTP fetch
    try:
        data = _extract_data(dhan.get_ltp(security_id=str(security_id), exchange_segment=exchange_segment))
        if isinstance(data, dict):
            if "last_price" in data:
                return _to_float(data.get("last_price"))
            if "last_traded_price" in data:
                return _to_float(data.get("last_traded_price"))
            if "ltp" in data:
                return _to_float(data.get("ltp"))
        if isinstance(data, list) and data:
            row = data[0]
            return _to_float(row.get("last_price", row.get("last_traded_price", row.get("ltp"))))
    except:
        pass
    return None


def _historical_daily(security_id, from_date, to_date):
    # BROKER CHANGE: Dhan historical daily fetch
    try:
        resp = dhan.historical_daily_data(
            security_id=str(security_id),
            exchange_segment=dhan.NSE_IDX,
            instrument_type="INDEX",
            from_date=from_date.strftime("%Y-%m-%d"),
            to_date=to_date.strftime("%Y-%m-%d")
        )
        rows = _extract_data(resp)
        if isinstance(rows, list):
            return rows
    except:
        pass
    return []


def _historical_intraday_5m(security_id, from_dt, to_dt):
    # BROKER CHANGE: Dhan intraday minute fetch
    try:
        resp = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment=dhan.NSE_IDX,
            instrument_type="INDEX",
            interval=5,
            from_date=from_dt.strftime("%Y-%m-%d"),
            to_date=to_dt.strftime("%Y-%m-%d")
        )
        rows = _extract_data(resp)
        if isinstance(rows, list):
            return rows
    except:
        pass
    return []


def _download_nfo_master():
    # BROKER CHANGE: Dhan security master for options mapping (security_id)
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    rows = []
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        lines = r.text.splitlines()
        if not lines:
            return rows
        headers = [h.strip() for h in lines[0].split(",")]
        for ln in lines[1:]:
            cols = ln.split(",")
            if len(cols) != len(headers):
                continue
            d = {headers[i]: cols[i].strip() for i in range(len(headers))}
            if d.get("SEM_EXM_EXCH_ID") == "NSE" and d.get("SEM_SEGMENT") == "FNO":
                rows.append(d)
    except Exception as e:
        print("Instrument download error:", e)
    return rows


def convert_to_dhan_symbol(sym):
    import re
    strike = re.findall(r"\d{5}", str(sym))
    if not strike:
        return sym
    strike = strike[0]
    if "CE" in str(sym):
        return f"NIFTY {strike} CE"
    return f"NIFTY {strike} PE"


def _is_dhan_order_failed(response):
    if response is None:
        return True
    if not isinstance(response, dict):
        return False
    status = str(response.get("status", response.get("orderStatus", ""))).upper()
    if status in {"FAILED", "REJECTED", "ERROR"}:
        return True
    if response.get("errorCode") or response.get("error"):
        return True
    return False


def dhan_place_order(symbol, side, qty):
    url = "https://api.dhan.co/orders"
    headers = {
        "access-token": DHAN_ACCESS_TOKEN if DHAN_ACCESS_TOKEN != "YOUR_ACCESS_TOKEN" else ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {
        "dhanClientId": DHAN_CLIENT_ID if DHAN_CLIENT_ID != "YOUR_CLIENT_ID" else CLIENT_ID,
        "transactionType": side,
        "exchangeSegment": "NSE_FNO",
        "productType": "INTRADAY",
        "orderType": "MARKET",
        "quantity": int(qty),
        "instrument": symbol,
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=5)
        return res.json()
    except Exception as e:
        print("[DHAN EXECUTION] Dhan order error:", e)
        return None


def _fallback_zerodha_order(sym, side, qty):
    try:
        if "kite" not in globals() or kite is None:
            print(f"[FALLBACK] Zerodha client unavailable for {sym} {side} {qty}")
            return None
        txn_type = kite.TRANSACTION_TYPE_BUY if side == "BUY" else kite.TRANSACTION_TYPE_SELL
        return kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NFO,
            tradingsymbol=sym,
            transaction_type=txn_type,
            quantity=int(qty),
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET
        )
    except Exception as e:
        print(f"[FALLBACK] Zerodha order error: {e}")
        return None


def execute_order(symbol, side, qty):
    global LAST_BROKER_USED
    import time

    start = time.time()

    if EXECUTION_MODE == "DHAN_PRIMARY":
        res = dhan_place_order(symbol, side, qty)

        if _is_dhan_order_failed(res):
            print("[FAILOVER] DHAN → ZERODHA")
            LAST_BROKER_USED = "ZERODHA"
            res = _fallback_zerodha_order(symbol, side, qty)
        else:
            LAST_BROKER_USED = "DHAN"

    elif EXECUTION_MODE == "ZERODHA_PRIMARY":
        res = _fallback_zerodha_order(symbol, side, qty)

        if not res:
            print("[FAILOVER] ZERODHA → DHAN")
            LAST_BROKER_USED = "DHAN"
            res = dhan_place_order(symbol, side, qty)
        else:
            LAST_BROKER_USED = "ZERODHA"

    elif EXECUTION_MODE == "DUAL_PARALLEL":
        print("[DUAL] BOTH BROKERS")

        res = {
            "dhan": dhan_place_order(symbol, side, qty),
            "zerodha": _fallback_zerodha_order(symbol, side, qty)
        }

    else:
        res = dhan_place_order(symbol, side, qty)
        LAST_BROKER_USED = "DHAN"

    latency = time.time() - start
    print(f"[EXECUTION] Mode={EXECUTION_MODE} Broker={LAST_BROKER_USED} Latency={latency:.3f}s")

    return res


print("Token test:", CLIENT_ID)
print("Downloading instruments...")
INSTRUMENTS=_download_nfo_master()
print("NFO instruments loaded")

# ================= OPTION INDEX CACHE =================
OPTION_INDEX = {}
NIFTY_EXPIRIES = []
NEXT_WEEK_EXPIRY = None

# ================= HEADER =================
def print_header():
    print(f"{GREEN}MODE: OPTION BUYING SCRIPT - LIVE TRADE{RESET}")
    print(f"{BLUE}Execution Date: {date.today()} | {datetime.now().strftime('%H:%M:%S')}{RESET}")
    send_telegram("🚀 Script Started")

# ================= SOUND =================
def sound_entry():
    if winsound:
        winsound.Beep(1200,300)

def sound_sl():
    if winsound:
        winsound.Beep(600,700)

def sound_target():
    if winsound:
        winsound.Beep(1500,250)

# ================= CPR + AUTO SIGNAL =================
# ================= CPR + AUTO SIGNAL (OPTIMIZED SINGLE FETCH) =================
def calculate_auto_signal():

    global AUTO_SIGNAL, allowed_side, MA_SIDE, CPR_TYPE, AUTO_READY

    today = date.today()

    # ⭐ SINGLE DAILY DATA FETCH (used for CPR + MA20)
    hist = _historical_daily(
        SPOT_TOKEN,
        today - timedelta(days=50),   # buffer for holidays
        today
    )

    if not hist or len(hist) < 22:
        print("Not enough daily candles — skipping AUTO SIGNAL")
        return

    # ==========================================================
    # ⭐ LAST COMPLETED DAY (avoid today running candle)
    # ==========================================================
    d = hist[-2]

    PDH = _to_float(d.get("high", d.get("High")))
    PDL = _to_float(d.get("low", d.get("Low")))
    PDC = _to_float(d.get("close", d.get("Close")))

    # ================= CPR =================
    pivot = (PDH + PDL + PDC) / 3
    BC = (PDH + PDL) / 2
    TC = (pivot - BC) + pivot

    cpr_width = abs(TC - BC) / pivot * 100

    if cpr_width >= 0.6:
        CPR_TYPE = "WIDE"
    elif cpr_width <= 0.15:
        CPR_TYPE = "NARROW"
    else:
        CPR_TYPE = "NORMAL"

    # ================= MA20 =================
    closes = [_to_float(i.get("close", i.get("Close"))) for i in hist[:-1]]   # exclude today
    ma20 = sum(closes[-20:]) / 20

    MA_SIDE = "Above" if PDC > ma20 else "Below"

    # ================= AUTO SIGNAL =================
    if CPR_TYPE != "WIDE":

        if MA_SIDE == "Above":
            AUTO_SIGNAL = "CE BUY DAY"
            allowed_side = "CE"
        else:
            AUTO_SIGNAL = "PE BUY DAY"
            allowed_side = "PE"

    else:
        AUTO_SIGNAL = "NO TRADE"

        # ⭐ Reverse logic for NO TRADE DAY
        if MA_SIDE == "Above":
            allowed_side = "PE"
        else:
            allowed_side = "CE"

    msg = (f"[AUTO SIGNAL] CPR={CPR_TYPE} | 20MA={MA_SIDE} | SIGNAL={AUTO_SIGNAL} | Allowed={allowed_side}")
    print(msg)
    send_telegram(msg)

    # ⭐ Unlock ENTRY engine
    AUTO_READY = True
# ================= EXPIRY =================

def _parse_expiry(x):
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(x, fmt).date()
        except:
            pass
    return None


def build_option_index():
    global OPTION_INDEX, NIFTY_EXPIRIES, NEXT_WEEK_EXPIRY

    option_index = {}
    expiry_set = set()
    today = date.today()

    for ins in INSTRUMENTS:
        if not ins.get("SEM_CUSTOM_SYMBOL", "").startswith("NIFTY"):
            continue

        expiry = _parse_expiry(ins.get("SEM_EXPIRY_DATE", ""))
        if not expiry or expiry < today:
            continue

        opt_type = ins.get("SEM_OPTION_TYPE")
        if opt_type not in ("CE", "PE"):
            continue

        try:
            strike = int(float(ins.get("SEM_STRIKE_PRICE", "0")))
        except:
            continue

        option_index[(expiry, strike, opt_type)] = (
            ins.get("SEM_TRADING_SYMBOL"),
            ins.get("SEM_SMST_SECURITY_ID"),
        )
        expiry_set.add(expiry)

    NIFTY_EXPIRIES = sorted(expiry_set)
    NEXT_WEEK_EXPIRY = NIFTY_EXPIRIES[1] if len(NIFTY_EXPIRIES) > 1 else (NIFTY_EXPIRIES[0] if NIFTY_EXPIRIES else None)
    OPTION_INDEX = option_index


def get_next_expiry():
    return NEXT_WEEK_EXPIRY


def get_atm_option_fast(spot, side):
    strike = round(spot / 50) * 50
    expiry = get_next_expiry()
    print(f"{YELLOW}Using NEXT WEEK Expiry: {expiry}{RESET}")
    if expiry is None:
        return None, None

    search_strikes = [strike, strike + 50, strike - 50, strike + 100, strike - 100]
    for s in search_strikes:
        hit = OPTION_INDEX.get((expiry, int(s), side))
        if hit:
            return hit
    return None, None


def get_atm_option(spot, side):
    return get_atm_option_fast(spot, side)


build_option_index()

# ================= FETCH =================
def fetch_spot():
    global spot_ltp
    try:
        spot_ltp=_get_ltp_by_security_id(SPOT_TOKEN, dhan.NSE_IDX)
    except Exception as e:
        print(f"[ZERODHA DATA] Spot fetch error: {e}")

# ================= 9:30 CANDLE =================
def fetch_930_candle():
    global candle_done

    if candle_done:
        return

    now = datetime.now().time()
    today = date.today()

    # ⭐ WAIT UNTIL 9:35
    if now < dtime(9,35):
        print("Market not opened yet – waiting for 9:30 candle")
        return

    data = _historical_intraday_5m(
        SPOT_TOKEN,
        datetime.combine(today, dtime(9,30)),
        datetime.combine(today, dtime(9,35))
    )

    # ⭐ AFTER 9:35 — if still no data → holiday
    if not data:
        print("No 9:30 candle data – possible holiday")
        sys.exit(0)

    candle["high"] = _to_float(data[0].get("high", data[0].get("High")))
    candle["low"] = _to_float(data[0].get("low", data[0].get("Low")))
    candle_done = True

    print(f"{GREEN}Fetched 9:30 candle successfully{RESET}")
    
    send_telegram("Fetched 9:30 candle successfully")

    pre_subscribe_atm_options()
    calculate_auto_signal()


def pre_subscribe_atm_options():
    global PRE_CE_TOKEN, PRE_PE_TOKEN, PRE_CE_SYMBOL, PRE_PE_SYMBOL

    if spot_ltp is None:
        return

    ce_sym, ce_tok = get_atm_option(spot_ltp, "CE")
    pe_sym, pe_tok = get_atm_option(spot_ltp, "PE")

    PRE_CE_SYMBOL, PRE_CE_TOKEN = ce_sym, ce_tok
    PRE_PE_SYMBOL, PRE_PE_TOKEN = pe_sym, pe_tok

    tokens = []
    if PRE_CE_TOKEN:
        tokens.append(PRE_CE_TOKEN)
    if PRE_PE_TOKEN and str(PRE_PE_TOKEN) != str(PRE_CE_TOKEN):
        tokens.append(PRE_PE_TOKEN)

    if tokens:
        kws.subscribe(tokens)
        kws.set_mode(kws.MODE_LTP, tokens)
        print(f"Pre-subscribed ATM options CE={PRE_CE_SYMBOL} PE={PRE_PE_SYMBOL}")
    

# ⭐⭐⭐ ADD PENDING ORDER FUNCTION HERE (OUTSIDE THE ABOVE FUNCTION)
def has_pending_order(sym):
    try:
        orders = _normalize_orders(dhan.get_order_list())
        for o in orders:
            if (
                o.get("tradingSymbol", o.get("tradingsymbol")) == sym and
                o.get("orderStatus", o.get("status")) in ["OPEN","TRIGGER PENDING","PUT ORDER REQ RECEIVED","PENDING"]
            ):
                return True
    except Exception as e:
        print("Order check error:", e)
    return False


# ================= LIVE ORDER BLOCK (SAFE VERSION) =================

# Global exit lock (prevents duplicate exits from heartbeat)
EXIT_DONE = False


# ================= LIVE BUY ORDER =================
def place_live_buy(sym):
    global EXIT_DONE, PENDING_ENTRY_SYMBOL, PENDING_ENTRY_FILL
    try:
        dhan_sym = convert_to_dhan_symbol(sym)
        response = execute_order(dhan_sym, "BUY", LOT_SIZE)
        print(f"[DHAN EXECUTION] DHAN BUY ORDER: {response}")

        if _is_dhan_order_failed(response):
            print("[FALLBACK] Fallback to Zerodha")
            response = _fallback_zerodha_order(sym, "BUY", LOT_SIZE)
            if not response:
                return False

        # ✅ Reset exit lock for new trade
        EXIT_DONE = False
        PENDING_ENTRY_SYMBOL = sym
        PENDING_ENTRY_FILL = True

        data = _extract_data(response)
        avg_price = None
        if isinstance(data, dict):
            avg_price = _to_float(
                data.get("averagePrice", data.get("average_price", data.get("tradedPrice", data.get("traded_price"))))
            )

        if avg_price:
            trade["entry_price"] = avg_price
            trade["prem_entry"] = avg_price
            PENDING_ENTRY_FILL = False

        trade["qty"] = int(LOT_SIZE)

        print(f"{GREEN}[DHAN EXECUTION] LIVE BUY ORDER PLACED : {sym} | {datetime.now().strftime('%H:%M:%S')}{RESET}")

        msg = f"""LIVE BUY ORDER PLACED
        {sym}
        Time: {datetime.now().strftime('%H:%M:%S')}"""
        send_telegram(msg)
        return True

    except Exception as e:
        print(f"BUY ORDER ERROR : {e}")
        return False


# ================= GET OPEN POSITION QTY =================
def get_open_qty(sym):
    global OPEN_QTY_CACHE
    try:
        now_ts = time.time()
        if (
            OPEN_QTY_CACHE["symbol"] == sym and
            (now_ts - OPEN_QTY_CACHE["ts"]) <= OPEN_QTY_CACHE_TTL
        ):
            return OPEN_QTY_CACHE["qty"]

        pos = _normalize_positions(dhan.get_positions())

        for p in pos:
            p_sym = p.get("tradingSymbol", p.get("tradingsymbol"))
            p_qty = abs(int(float(p.get("netQty", p.get("quantity", 0)))))
            if p_sym == sym and p_qty > 0:
                OPEN_QTY_CACHE = {"symbol": sym, "qty": p_qty, "ts": now_ts}
                return p_qty

        OPEN_QTY_CACHE = {"symbol": sym, "qty": 0, "ts": now_ts}
        return 0

    except Exception as e:
        print("Position fetch error :", e)
        return None   # ⭐ IMPORTANT CHANGE


# ================= POSITION RECOVERY (ADD THIS BELOW) =================
def recover_position():

    global trade_open, ACTIVE_SYMBOL

    try:
        pos = _normalize_positions(dhan.get_positions())

        for p in pos:
            p_qty = abs(int(float(p.get("netQty", p.get("quantity", 0)))))
            p_product = p.get("productType", p.get("product"))
            if p_qty > 0 and p_product == PRODUCT:

                trade_open = True
                ACTIVE_SYMBOL = p.get("tradingSymbol", p.get("tradingsymbol"))

                print("Recovered existing position:", ACTIVE_SYMBOL)

                return

    except Exception as e:
        print("Recovery error:", e)


# ================= SAFE EXIT ORDER =================
def place_live_exit(sym):
    global EXIT_DONE, OPEN_QTY_CACHE

    try:
        # 🚫 Prevent duplicate exit
        if EXIT_DONE:
            print("Exit already done — skipping")
            return

        qty = int(trade.get("qty", 0))
        print("Exit Qty :", qty)

        # 🚫 No position exists
        if qty == 0:
            print("No trade quantity cached — exit skipped")
            return

        dhan_sym = convert_to_dhan_symbol(sym)
        response = execute_order(dhan_sym, "SELL", qty)
        print(f"[DHAN EXECUTION] DHAN EXIT ORDER: {response}")
        if _is_dhan_order_failed(response):
            print("[FALLBACK] Fallback to Zerodha")
            response = _fallback_zerodha_order(sym, "SELL", qty)
            if not response:
                print("[FALLBACK] Zerodha exit also failed")
                return

        EXIT_DONE = True
        OPEN_QTY_CACHE = {"symbol": sym, "qty": 0, "ts": time.time()}
        print(f"{RED}[DHAN EXECUTION] LIVE EXIT ORDER : {sym}{RESET}")

    except Exception as e:
        print(f"EXIT ORDER ERROR : {e}")


# ================= WEBSOCKET =================
# BROKER CHANGE: Dhan feed adapter to preserve KiteTicker-style callbacks
class DhanTickerAdapter:
    MODE_LTP = "LTP"

    def __init__(self, client_id, access_token):
        self.client_id = client_id
        self.access_token = access_token
        self._subs = set()
        self._running = False
        self.on_ticks = None
        self.on_connect = None
        self.on_close = None
        self._thread = None

    def _to_feed_tuple(self, security_id):
        sid = str(security_id)
        if sid == str(SPOT_TOKEN):
            return (MarketFeed.IDX, sid, MarketFeed.Ticker)
        return (MarketFeed.NSE_FNO, sid, MarketFeed.Ticker)

    def subscribe(self, tokens):
        for t in tokens:
            self._subs.add(str(t))

    def set_mode(self, mode, tokens):
        return

    def stop(self):
        self._running = False

    def connect(self, threaded=True):
        self._running = True

        def _runner():
            if self.on_connect:
                self.on_connect(self, None)
            while self._running:
                if not self._subs:
                    time.sleep(0.2)
                    continue
                instruments = [self._to_feed_tuple(s) for s in sorted(self._subs)]
                try:
                    feed = MarketFeed(_dhan_context, instruments, "v2")
                    ws_thread = threading.Thread(target=feed.run_forever, daemon=True)
                    ws_thread.start()
                    last_tick_ts = time.time()
                    while self._running:
                        tick = feed.get_data()
                        if not tick:
                            if time.time() - last_tick_ts > 3:
                                raise ConnectionError("Feed stalled; reconnecting")
                            time.sleep(0.01)
                            continue

                        secid = str(tick.get("security_id", tick.get("securityId", "")))
                        ltp = _to_float(tick.get("LTP", tick.get("last_price", tick.get("ltp"))))
                        if secid and ltp is not None and self.on_ticks:
                            last_tick_ts = time.time()
                            self.on_ticks(self, [{"instrument_token": secid, "last_price": ltp}])
                except Exception as e:
                    if day_closed:
                        break
                    print(f"Feed reconnect: {e}")
                    time.sleep(0.5)
            if self.on_close:
                self.on_close(self, None, None)

        if threaded:
            self._thread = threading.Thread(target=_runner, daemon=True)
            self._thread.start()
        else:
            _runner()


def on_connect(ws,r):
    print("WebSocket connected")
    ws.subscribe([SPOT_TOKEN])
    ws.set_mode(ws.MODE_LTP,[SPOT_TOKEN])
    print("WebSocket connected")

def on_close(ws, c, r):

    # 🚫 Never reconnect after day close
    if day_closed:
        print("WebSocket closed (day finished)")
        return

    print("WebSocket closed - waiting auto reconnect")

# ================= CORE ENGINE =================
def on_ticks(ws, ticks):

    # ⭐ ADD THIS LINE HERE (FIRST THING INSIDE FUNCTION)
    if ws is None:
        ws = kws

    global trade_open, ACTIVE_OPTION_TOKEN, ACTIVE_SYMBOL, ACTIVE_SIDE
    global ORDER_PLACED, BLOCK_MSG_SHOWN
    global spot_ltp, option_ltp, day_closed, LAST_OPTION_TICK_TS, PENDING_ENTRY_SYMBOL
    global LAST_SPOT_TICK_TS, LAST_ANY_TICK_TS, PENDING_ENTRY_FILL

    now = datetime.now().time()
    tick_now_ts = time.time()

    # ===== UPDATE LTP =====
    for t in ticks:
        if "last_price" in t and str(t.get("instrument_token")) == str(SPOT_TOKEN):
            spot_ltp = t["last_price"]
            LAST_SPOT_TICK_TS = tick_now_ts
            LAST_ANY_TICK_TS = tick_now_ts
        if ACTIVE_OPTION_TOKEN and str(t.get("instrument_token")) == str(ACTIVE_OPTION_TOKEN):
            option_ltp = t["last_price"]
            LAST_OPTION_TICK_TS = tick_now_ts
            LAST_ANY_TICK_TS = tick_now_ts
            if trade_open and PENDING_ENTRY_FILL and PENDING_ENTRY_SYMBOL == ACTIVE_SYMBOL and "entry_price" not in trade:
                trade["entry_price"] = option_ltp
                trade["prem_entry"] = option_ltp
                PENDING_ENTRY_FILL = False

    if not candle_done or day_closed:
        return

    # ===== UNIVERSAL DAY CLOSE (3:20 PM) =====
# ===== UNIVERSAL DAY CLOSE (3:20 PM) =====
    if now >= FORCE_EXIT_TIME and not day_closed:

        print(f"{RED}3:20 PM DAY CLOSE TRIGGERED{RESET}")

        if trade_open and ACTIVE_SYMBOL:
            print(f"{YELLOW}Closing active trade...{RESET}")
            if int(trade.get("qty", 0)) > 0:
                place_live_exit(ACTIVE_SYMBOL)
                print(f"{RED}Position closed for day end{RESET}")
            else:
                print("Position already closed manually") 
        else:
            print(f"{BLUE}No running trade - closing script for the day{RESET}")

        print(f"{GREEN}DAY COMPLETED{RESET}")

        send_telegram("DAY COMPLETED")

        day_closed = True
        globals()["SCRIPT_RUNNING"] = False

        safe_kws_stop()        # ⭐ IMPORTANT (use stop, not close)
        return


    # ===== ENTRY =====
    if not trade_open and not ORDER_PLACED and spot_ltp and now < LAST_ENTRY_TIME:

        #⭐ AUTO SIGNAL LOCK (ADD THIS)
        if not AUTO_READY:
            return


        if CPR_TYPE == "WIDE":
           return

        if allowed_side is None:
            return

        side = None

        if spot_ltp >= candle["high"] + 1:
            if allowed_side == "CE":
                side = "CE"
                BLOCK_MSG_SHOWN = False
            else:
                if not BLOCK_MSG_SHOWN:
                    print(f"{YELLOW}ENTRY BLOCKED – CE not allowed{RESET}")
                    BLOCK_MSG_SHOWN = True
                return

        elif spot_ltp <= candle["low"] - 1:
            if allowed_side == "PE":
                side = "PE"
                BLOCK_MSG_SHOWN = False
            else:
                if not BLOCK_MSG_SHOWN:
                    print(f"{YELLOW}ENTRY BLOCKED – PE not allowed{RESET}")
                    BLOCK_MSG_SHOWN = True
                return
        else:
            return

        if side == "CE":
            sym, tok = PRE_CE_SYMBOL, PRE_CE_TOKEN
        else:
            sym, tok = PRE_PE_SYMBOL, PRE_PE_TOKEN

        if sym is None or tok is None:
            sym, tok = get_atm_option(spot_ltp, side)

        ACTIVE_OPTION_TOKEN = tok
        ACTIVE_SYMBOL = sym
        ACTIVE_SIDE = side
        if ACTIVE_SYMBOL is None:
           print("ATM option not found — skipping entry")
           return

        if ws:
            ws.subscribe([tok])
            ws.set_mode(ws.MODE_LTP, [tok])

        print(f"{BLUE}Trade Executed Date: {date.today()} | {datetime.now().strftime('%H:%M:%S')}{RESET}")
        trade.clear()

        buy_ok = place_live_buy(sym)
        if not buy_ok:
            ACTIVE_OPTION_TOKEN = None
            ACTIVE_SYMBOL = None
            ACTIVE_SIDE = None
            return

        ORDER_PLACED = True
        trade_open = True
        sound_entry()

# ===== MANAGEMENT =====
    # ===== MANAGEMENT =====
    if trade_open:
        if (time.time() - LAST_OPTION_TICK_TS) > 2:
            return

        if option_ltp is None:
            return

        if "prem_entry" not in trade:
            if trade.get("entry_price"):
                trade["prem_entry"] = trade["entry_price"]
            elif PENDING_ENTRY_SYMBOL == ACTIVE_SYMBOL and option_ltp is not None:
                trade["prem_entry"] = option_ltp
                trade["entry_price"] = option_ltp
                PENDING_ENTRY_SYMBOL = None
                PENDING_ENTRY_FILL = False
            else:
                if option_ltp is not None:
                    trade["prem_entry"] = option_ltp
                    trade["entry_price"] = option_ltp
                    PENDING_ENTRY_SYMBOL = None
                    PENDING_ENTRY_FILL = False
                else:
                    return

            if "qty" not in trade:
                trade["qty"] = int(LOT_SIZE)

            if trade["prem_entry"] is None:
                return

            trade["prem_sl"] = round(trade["prem_entry"] - PREM_SL_PTS,2)
            trade["prem_target"] = round(trade["prem_entry"] + PREM_TGT_PTS,2)

            print(f"Premium Entry (FILLED): {trade['prem_entry']}")
            print(f"Target : {trade['prem_target']} | SL : {trade['prem_sl']}")

            msg = f"""Premium Entry: {trade['prem_entry']}
            Target: {trade['prem_target']}
            SL: {trade['prem_sl']}"""
            send_telegram(msg)
            return

        if option_ltp <= trade["prem_sl"]:
            reason = "SL"
            sound_sl()

        elif option_ltp >= trade["prem_target"]:
            reason = "TARGET"
            sound_target()

        else:
            return

        place_live_exit(ACTIVE_SYMBOL)
        print(f"Exit Trade - {reason}")
   
        msg = f"""EXIT TRADE
        Symbol: {ACTIVE_SYMBOL}
        Reason: {reason}
        Time: {datetime.now().strftime('%H:%M:%S')}"""
        send_telegram(msg)

        day_closed = True
        SCRIPT_RUNNING = False
        trade["qty"] = 0
        safe_kws_stop()
        return

# ⭐⭐⭐ ADD HERE ⭐⭐⭐

def safe_kws_stop():
    global WS_STOPPED
    if WS_STOPPED:
        return
    try:
        kws.stop()
    except:
        pass
    WS_STOPPED = True

# ================= START =================

print_header()

recover_position()

kws = DhanTickerAdapter(CLIENT_ID, ACCESS_TOKEN)
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

kws.connect(threaded=True)


def heartbeat():

    while SCRIPT_RUNNING:

        # Fetch spot price
        fetch_spot()

        # Fetch 9:30 candle once
        if not candle_done and datetime.now().time() > dtime(9,35):
            fetch_930_candle()

        # Heartbeat delay
        time.sleep(1)


# Start background heartbeat
threading.Thread(target=heartbeat, daemon=True).start()


# Keep script alive
while SCRIPT_RUNNING:
    time.sleep(1)


print("Script exited cleanly")
send_telegram("🛑 Script Stopped")
# Remove lock file
if os.path.exists(LOCK_FILE):
    os.remove(LOCK_FILE)

sys.exit(0)
