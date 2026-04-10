# ==========================================================
# ULTRA-PRO OPTION BUYING – LIVE TRADE VERSION
# (Same Framework – LIVE ORDER EXECUTION ADDED)
# ==========================================================

# ================= CONFIG =================
API_KEY ="ikzievanmkaoalmz"
ACCESS_TOKEN ="927X2ijccpAleBJKsAFvIe9OXBvpMSoS"
# ==========================================================



from kiteconnect import KiteConnect, KiteTicker
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
PRODUCT="MIS"
EXCHANGE="NFO"
ORDER_TYPE="MARKET"

SPOT_TOKEN=256265
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
ORDER_PLACED=False
BLOCK_MSG_SHOWN=False
day_closed = False
SCRIPT_RUNNING = True
WS_STOPPED = False

AUTO_SIGNAL="NO TRADE"
allowed_side=None
MA_SIDE=None
CPR_TYPE=None
AUTO_READY = False

trade={}
day_closed=False

candle={"high":None,"low":None}
candle_done=False

# ================= KITE =================
kite=KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

print("Token test:",kite.profile()["user_name"])
print("Downloading instruments...")
INSTRUMENTS=kite.instruments("NFO")
print("NFO instruments loaded")

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
    hist = kite.historical_data(
        SPOT_TOKEN,
        today - timedelta(days=50),   # buffer for holidays
        today,
        "day"
    )

    if not hist or len(hist) < 22:
        print("Not enough daily candles — skipping AUTO SIGNAL")
        return

    # ==========================================================
    # ⭐ LAST COMPLETED DAY (avoid today running candle)
    # ==========================================================
    d = hist[-2]

    PDH = d["high"]
    PDL = d["low"]
    PDC = d["close"]

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
    closes = [i["close"] for i in hist[:-1]]   # exclude today
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

def get_next_expiry():
    today = date.today()

    expiries = sorted(set(
        i["expiry"] for i in INSTRUMENTS
        if i["name"] == "NIFTY" and i["expiry"] >= today
    ))

    # ⭐ Always choose NEXT WEEK expiry
    if len(expiries) > 1:
        return expiries[1]     # second expiry = next week
    else:
        return expiries[0]     # fallback safety


def get_atm_option(spot,side):
    strike=round(spot/50)*50
    expiry=get_next_expiry()
    print(f"{YELLOW}Using NEXT WEEK Expiry: {expiry}{RESET}")
    for i in INSTRUMENTS:
        if i["name"]=="NIFTY" and i["expiry"]==expiry and i["strike"]==strike and i["instrument_type"]==side:
            return i["tradingsymbol"],i["instrument_token"]

# ================= FETCH =================
def fetch_spot():
    global spot_ltp
    try:
        spot_ltp=kite.ltp(["NSE:NIFTY 50"])["NSE:NIFTY 50"]["last_price"]
    except: pass

def fetch_option_ltp():
    global option_ltp
    try:
        option_ltp=list(kite.ltp([ACTIVE_OPTION_TOKEN]).values())[0]["last_price"]
    except: pass

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

    data = kite.historical_data(
        SPOT_TOKEN,
        datetime.combine(today, dtime(9,30)),
        datetime.combine(today, dtime(9,35)),
        "5minute"
    )

    # ⭐ AFTER 9:35 — if still no data → holiday
    if not data:
        print("No 9:30 candle data – possible holiday")
        sys.exit(0)

    candle["high"] = data[0]["high"]
    candle["low"] = data[0]["low"]
    candle_done = True

    print(f"{GREEN}Fetched 9:30 candle successfully{RESET}")
    
    send_telegram("Fetched 9:30 candle successfully")

    calculate_auto_signal()
    

# ⭐⭐⭐ ADD PENDING ORDER FUNCTION HERE (OUTSIDE THE ABOVE FUNCTION)
def has_pending_order(sym):
    try:
        orders = kite.orders()
        for o in orders:
            if (
                o["tradingsymbol"] == sym and
                o["status"] in ["OPEN","TRIGGER PENDING","PUT ORDER REQ RECEIVED"]
            ):
                return True
    except Exception as e:
        print("Order check error:", e)
    return False


# ⭐⭐⭐ ADD HERE (same indentation level)
# ================= GET LAST FILLED BUY PRICE =================
def get_last_fill_price(sym):
    try:
        orders = kite.orders()[::-1]
        for o in orders:
            if (
                o["tradingsymbol"] == sym and
                o["transaction_type"] == "BUY" and
                o["status"] == "COMPLETE"
            ):
                return float(o["average_price"])
    except Exception as e:
        print("Fill price fetch error:", e)
    return None


# ================= LIVE ORDER BLOCK (SAFE VERSION) =================

# Global exit lock (prevents duplicate exits from heartbeat)
EXIT_DONE = False


# ================= LIVE BUY ORDER =================
def place_live_buy(sym):
    global EXIT_DONE
    try:
        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=LOT_SIZE,
            product=PRODUCT,
            order_type=ORDER_TYPE
        )

        # ✅ Reset exit lock for new trade
        EXIT_DONE = False

        print(f"{GREEN}LIVE BUY ORDER PLACED : {sym} | {datetime.now().strftime('%H:%M:%S')}{RESET}")

        msg = f"""LIVE BUY ORDER PLACED
        {sym}
        Time: {datetime.now().strftime('%H:%M:%S')}"""
        send_telegram(msg)

    except Exception as e:
        print(f"BUY ORDER ERROR : {e}")


# ================= GET OPEN POSITION QTY =================
def get_open_qty(sym):

    try:
        pos = kite.positions()["net"]

        for p in pos:
            if p["tradingsymbol"] == sym and abs(p["quantity"]) > 0:
                return abs(p["quantity"])

        return 0

    except Exception as e:
        print("Position fetch error :", e)
        return None   # ⭐ IMPORTANT CHANGE


# ================= POSITION RECOVERY (ADD THIS BELOW) =================
def recover_position():

    global trade_open, ACTIVE_SYMBOL

    try:
        pos = kite.positions()["net"]

        for p in pos:
            if abs(p["quantity"]) > 0 and p["product"] == PRODUCT:

                trade_open = True
                ACTIVE_SYMBOL = p["tradingsymbol"]

                print("Recovered existing position:", ACTIVE_SYMBOL)

                return

    except Exception as e:
        print("Recovery error:", e)


# ================= SAFE EXIT ORDER =================
def place_live_exit(sym):
    global EXIT_DONE

    try:
        # 🚫 Prevent duplicate exit
        if EXIT_DONE:
            print("Exit already done — skipping")
            return

        qty = get_open_qty(sym)
        print("Open Qty Found :", qty)

        # 🚫 No position exists
        if qty == 0:
            print("No open position — exit skipped")
            return

        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=qty,   # ✅ Use actual open quantity
            product=PRODUCT,
            order_type=ORDER_TYPE
        )

        EXIT_DONE = True
        print(f"{RED}LIVE EXIT ORDER : {sym}{RESET}")

    except Exception as e:
        print(f"EXIT ORDER ERROR : {e}")


# ================= WEBSOCKET =================
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

    global trade_open, ACTIVE_OPTION_TOKEN, ACTIVE_SYMBOL
    global ORDER_PLACED, BLOCK_MSG_SHOWN
    global spot_ltp, option_ltp, day_closed

    now = datetime.now().time()

    # ===== UPDATE LTP =====
    for t in ticks:
        if "last_price" in t:
            spot_ltp = t["last_price"]
        if ACTIVE_OPTION_TOKEN and t.get("instrument_token") == ACTIVE_OPTION_TOKEN:
            option_ltp = t["last_price"]

    if not candle_done or day_closed:
        return

    # ===== UNIVERSAL DAY CLOSE (3:20 PM) =====
# ===== UNIVERSAL DAY CLOSE (3:20 PM) =====
    if now >= FORCE_EXIT_TIME and not day_closed:

        print(f"{RED}3:20 PM DAY CLOSE TRIGGERED{RESET}")

        if trade_open and ACTIVE_SYMBOL:
            print(f"{YELLOW}Closing active trade...{RESET}")

            qty = get_open_qty(ACTIVE_SYMBOL)
            if qty > 0:
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

        sym, tok = get_atm_option(spot_ltp, side)

        ACTIVE_OPTION_TOKEN = tok
        ACTIVE_SYMBOL = sym
        # ⭐ ADD THIS SAFETY CHECK HERE
        if ACTIVE_SYMBOL is None:
           print("ATM option not found — skipping entry")
           return

        # ⭐ POSITION SAFETY (ADD THIS)
        if get_open_qty(sym) > 0:
            print("Position already exists — skipping entry")
            return

        if ws:
            ws.subscribe([tok])
            ws.set_mode(ws.MODE_LTP, [tok])

        print(f"{BLUE}Trade Executed Date: {date.today()} | {datetime.now().strftime('%H:%M:%S')}{RESET}")
   
        # ⭐ Pending order protection
        if has_pending_order(sym):
            print("Order already pending — skipping duplicate entry")
            return

        ORDER_PLACED = True
        trade_open = True
        trade.clear()

        place_live_buy(sym)
        sound_entry()

# ===== MANAGEMENT =====
    # ===== MANAGEMENT =====
    if trade_open:

        fetch_option_ltp()
        if option_ltp is None:
            return

        # ⭐ Detect manual exit (SAFE VERSION)
        qty = get_open_qty(ACTIVE_SYMBOL)
         
        if qty is None:
            return

        if qty == 0:
            print("Manual exit detected — resetting trade state")
            trade_open = False
            return

        qty = get_open_qty(ACTIVE_SYMBOL)
        # API failure → skip check
        if qty is None:
            return 

        # True manual exit
        if qty == 0:
            print("Manual exit detected — resetting trade state")
            trade_open = False
            return

        # ⭐ Fill-price entry logic INSIDE trade_open
        if "prem_entry" not in trade:

            fill_price = get_last_fill_price(ACTIVE_SYMBOL)

            if fill_price:
                trade["prem_entry"] = fill_price
            else:
                trade["prem_entry"] = option_ltp

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

kws = KiteTicker(API_KEY, ACCESS_TOKEN)
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