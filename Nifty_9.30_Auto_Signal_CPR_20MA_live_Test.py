"""
NIFTY 9:30 Candle Breakout Bot (PURE DHAN API)
------------------------------------------------
Strategy kept intact:
- 9:30 candle breakout on NIFTY spot
- One trade per day
- CE/PE side chosen by CPR + 20MA filter
- Premium SL/Target management
- Intraday square-off at FORCE_EXIT_TIME

Notes:
- Uses ONLY Dhan APIs (REST + marketfeed)
- No Zerodha/Kite imports or symbols
- No asyncio/event-loop usage (thread-safe polling model)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
import atexit
import os
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
from colorama import init
from dhanhq import dhanhq
from dhanhq import marketfeed


# ========================== CONFIG ==========================
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "YOUR_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")

MODE = "LIVE"
PRODUCT = "INTRADAY"
EXCHANGE_SEGMENT_OPT = "NSE_FNO"
ORDER_TYPE = "MARKET"

# Dhan security IDs
SPOT_SECURITY_ID = "13"  # NIFTY 50 index spot
LOT_SIZE = 130

PREM_SL_PTS = 20
PREM_TGT_PTS = 40

LAST_ENTRY_TIME = dtime(15, 15)
FORCE_EXIT_TIME = dtime(15, 20)
CPR_WIDE_THRESHOLD = 0.6

# WebSocket reconnect controls
RECONNECT_BASE_DELAY = 3
RECONNECT_MAX_DELAY = 30
MAX_EMPTY_TICKS = 120
EMPTY_TICK_LOG_EVERY = 20
REST_FALLBACK_INTERVAL_SEC = 5
MAX_RECONNECT_ATTEMPTS = 10
TRADE_COOLDOWN_SEC = 10
TRADE_LOG_FILE = "trade_log.xlsx"

# lock file
LOCK_FILE = "trading.lock"

# telegram (optional)
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")


# ========================== UI ==========================
init(autoreset=True)
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"


def send_telegram(msg: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": f"<pre>{msg}</pre>", "parse_mode": "HTML"}
        requests.post(url, data=payload, timeout=5)
    except Exception as exc:
        print(f"Telegram Error: {exc}")


def remove_lock() -> None:
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)


def ensure_single_instance() -> None:
    if os.path.exists(LOCK_FILE):
        print("Script already running — exiting")
        sys.exit(0)
    open(LOCK_FILE, "w").close()


# ========================== HELPERS ==========================
def _to_float(value) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


@dataclass
class Candle:
    high: float
    low: float


class DhanRestClient:
    """Small REST wrapper for endpoints needed by this strategy."""

    def __init__(self, client_id: str, access_token: str):
        self.client_id = client_id
        self.access_token = access_token
        self.base = "https://api.dhan.co/v2"
        self.headers = {
            "access-token": access_token,
            "Content-Type": "application/json",
        }
        self.auth_failed = False

    def _request(self, method: str, path: str, payload: Optional[dict] = None) -> Optional[dict]:
        try:
            url = f"{self.base}{path}"
            resp = requests.request(method, url, headers=self.headers, json=payload, timeout=10)

            if resp.status_code == 401:
                print("[AUTH ERROR] Access token expired/invalid.")
                self.auth_failed = True
                return None

            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, dict) else {"data": data}
        except requests.exceptions.RequestException as exc:
            print(f"[NETWORK ERROR] {path}: {exc}")
            return None
        except Exception as exc:
            print(f"[REST ERROR] {path}: {exc}")
            return None

    def get_ltp(self, security_id: str, exchange_segment: str) -> Optional[float]:
        payload = {
            "NSE_EQ": [],
            "NSE_FNO": [],
            "NSE_CURRENCY": [],
            "BSE_EQ": [],
            "MCX_COMM": [],
            "IDX_I": [],
        }
        if exchange_segment == "IDX_I":
            payload["IDX_I"] = [str(security_id)]
        elif exchange_segment == "NSE_FNO":
            payload["NSE_FNO"] = [str(security_id)]
        else:
            payload["NSE_EQ"] = [str(security_id)]

        raw = self._request("POST", "/marketfeed/ltp", payload)
        if not raw:
            return None

        # Dhan returns nested map keyed by segment->security_id
        for segment_map in raw.values():
            if isinstance(segment_map, dict):
                row = segment_map.get(str(security_id))
                if isinstance(row, dict):
                    return _to_float(row.get("last_price") or row.get("LTP") or row.get("ltp"))
        return None

    def historical_daily(self, security_id: str, from_date: date, to_date: date) -> List[dict]:
        payload = {
            "securityId": str(security_id),
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX",
            "fromDate": from_date.strftime("%Y-%m-%d"),
            "toDate": to_date.strftime("%Y-%m-%d"),
        }
        raw = self._request("POST", "/charts/historical", payload)
        if not raw:
            return []
        return raw.get("data", []) if isinstance(raw.get("data", []), list) else []

    def historical_intraday_5m(self, security_id: str, on_date: date) -> List[dict]:
        payload = {
            "securityId": str(security_id),
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX",
            "interval": "5",
            "fromDate": on_date.strftime("%Y-%m-%d"),
            "toDate": on_date.strftime("%Y-%m-%d"),
        }
        raw = self._request("POST", "/charts/intraday", payload)
        if not raw:
            return []
        return raw.get("data", []) if isinstance(raw.get("data", []), list) else []

    def place_order(self, security_id: str, side: str, qty: int) -> Optional[dict]:
        payload = {
            "dhanClientId": self.client_id,
            "transactionType": side,
            "exchangeSegment": EXCHANGE_SEGMENT_OPT,
            "productType": PRODUCT,
            "orderType": ORDER_TYPE,
            "validity": "DAY",
            "securityId": str(security_id),
            "quantity": int(qty),
            "price": 0,
            "triggerPrice": 0,
            "afterMarketOrder": False,
        }
        return self._request("POST", "/orders", payload)


# ========================== STRATEGY STATE ==========================
class BotState:
    def __init__(self):
        self.spot_ltp: Optional[float] = None
        self.option_ltp: Optional[float] = None

        self.trade_open = False
        self.order_placed = False
        self.day_closed = False
        self.auto_ready = False

        self.allowed_side: Optional[str] = None
        self.auto_signal = "NO TRADE"
        self.cpr_type: Optional[str] = None
        self.ma_side: Optional[str] = None

        self.candle: Optional[Candle] = None

        self.active_symbol: Optional[str] = None
        self.active_security_id: Optional[str] = None
        self.active_side: Optional[str] = None

        self.prem_entry: Optional[float] = None
        self.prem_sl: Optional[float] = None
        self.prem_target: Optional[float] = None

        self.last_option_tick_ts = 0.0
        self.feed_resubscribe_required = False
        self.last_spot_log_ts = 0.0
        self.last_option_log_ts = 0.0
        self.last_trade_time = 0.0
        self.reconnect_failures = 0

        self.entry_time: Optional[datetime] = None
        self.exit_time: Optional[datetime] = None
        self.exit_reason: Optional[str] = None
        self.trade_logged = False


state = BotState()


# ========================== INSTRUMENTS ==========================
def _parse_expiry(value: str) -> Optional[date]:
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except Exception:
            continue
    return None


def download_nfo_master() -> List[dict]:
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    rows: List[dict] = []
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        lines = resp.text.splitlines()
        if not lines:
            return rows
        headers = [h.strip() for h in lines[0].split(",")]
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) != len(headers):
                continue
            record = {headers[i]: cols[i].strip() for i in range(len(headers))}
            if record.get("SEM_EXM_EXCH_ID") == "NSE" and record.get("SEM_SEGMENT") == "FNO":
                rows.append(record)
    except Exception as exc:
        print(f"Instrument download error: {exc}")
    return rows


def download_nfo_master_with_retry() -> List[dict]:
    rows = download_nfo_master()
    if rows:
        return rows
    print(f"{YELLOW}Instrument download failed/empty. Retrying once...{RESET}")
    time.sleep(1)
    return download_nfo_master()


def build_option_index(instruments: List[dict]) -> Tuple[Dict[Tuple[date, int, str], dict], Optional[date]]:
    option_index: Dict[Tuple[date, int, str], dict] = {}
    expiries = set()
    today = date.today()

    for ins in instruments:
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
        except Exception:
            continue

        option_index[(expiry, strike, opt_type)] = {
            "symbol": ins.get("SEM_TRADING_SYMBOL"),
            "security_id": ins.get("SEM_SMST_SECURITY_ID"),
        }
        expiries.add(expiry)

    sorted_exp = sorted(expiries)
    next_week = sorted_exp[1] if len(sorted_exp) > 1 else (sorted_exp[0] if sorted_exp else None)
    return option_index, next_week


def get_atm_option(spot: float, side: str, option_index: dict, expiry: date) -> Tuple[Optional[str], Optional[str]]:
    strike = round(spot / 50) * 50
    for s in [strike, strike + 50, strike - 50, strike + 100, strike - 100]:
        row = option_index.get((expiry, int(s), side))
        if row:
            return row["symbol"], str(row["security_id"])
    return None, None


# ========================== STRATEGY LOGIC ==========================
def calculate_auto_signal(rest: DhanRestClient) -> None:
    today = date.today()
    candles = rest.historical_daily(SPOT_SECURITY_ID, today - timedelta(days=60), today)

    if len(candles) < 22:
        print("Not enough daily candles — skipping AUTO SIGNAL")
        return

    prev = candles[-2]
    pdh = _to_float(prev.get("high") or prev.get("High"))
    pdl = _to_float(prev.get("low") or prev.get("Low"))
    pdc = _to_float(prev.get("close") or prev.get("Close"))

    if pdh is None or pdl is None or pdc is None:
        print("Invalid previous-day OHLC data")
        return

    pivot = (pdh + pdl + pdc) / 3
    bc = (pdh + pdl) / 2
    tc = (pivot - bc) + pivot

    cpr_width = abs(tc - bc) / pivot * 100
    state.cpr_type = "WIDE" if cpr_width >= CPR_WIDE_THRESHOLD else ("NARROW" if cpr_width <= 0.15 else "NORMAL")

    closes = [_to_float(x.get("close") or x.get("Close")) for x in candles[:-1]]
    closes = [x for x in closes if x is not None]
    if len(closes) < 20:
        print("Not enough closes for MA20")
        return
    ma20 = sum(closes[-20:]) / 20

    state.ma_side = "Above" if pdc > ma20 else "Below"

    if state.cpr_type != "WIDE":
        if state.ma_side == "Above":
            state.auto_signal = "CE BUY DAY"
            state.allowed_side = "CE"
        else:
            state.auto_signal = "PE BUY DAY"
            state.allowed_side = "PE"
    else:
        state.auto_signal = "NO TRADE"
        state.allowed_side = "PE" if state.ma_side == "Above" else "CE"

    state.auto_ready = True
    msg = f"[AUTO SIGNAL] CPR={state.cpr_type} | 20MA={state.ma_side} | SIGNAL={state.auto_signal} | Allowed={state.allowed_side}"
    print(msg)
    send_telegram(msg)


def fetch_930_candle(rest: DhanRestClient) -> Optional[Candle]:
    today = date.today()
    data = rest.historical_intraday_5m(SPOT_SECURITY_ID, today)
    if not data:
        print("No intraday data received — holiday/API issue")
        return None

    # locate 09:30 candle
    for row in data:
        ts = row.get("timestamp") or row.get("time") or row.get("start_Time")
        if not ts:
            continue
        ts_text = str(ts)
        if "09:30" in ts_text:
            high = _to_float(row.get("high") or row.get("High"))
            low = _to_float(row.get("low") or row.get("Low"))
            if high is not None and low is not None:
                return Candle(high=high, low=low)

    # fallback to first candle if timestamp format differs
    first = data[0]
    high = _to_float(first.get("high") or first.get("High"))
    low = _to_float(first.get("low") or first.get("Low"))
    if high is not None and low is not None:
        return Candle(high=high, low=low)

    return None


def should_enter(side: str) -> bool:
    if state.candle is None or state.spot_ltp is None:
        return False

    if side == "CE":
        return state.spot_ltp >= state.candle.high + 1
    return state.spot_ltp <= state.candle.low - 1


def place_entry(rest: DhanRestClient, symbol: str, security_id: str, side: str) -> bool:
    if time.time() - state.last_trade_time < TRADE_COOLDOWN_SEC:
        print(
            f"{YELLOW}[ENTRY BLOCKED] Cooldown active ({TRADE_COOLDOWN_SEC}s). "
            f"Preventing duplicate trade trigger.{RESET}"
        )
        return False

    response = rest.place_order(security_id=security_id, side="BUY", qty=LOT_SIZE)
    if not response:
        print("[ENTRY FAILED] Empty order response")
        return False

    if response.get("status") in {"failure", "rejected", "error"}:
        print(f"[ENTRY FAILED] {response}")
        return False

    state.active_symbol = symbol
    state.active_security_id = security_id
    state.active_side = side
    state.trade_open = True
    state.order_placed = True
    state.feed_resubscribe_required = True
    state.last_trade_time = time.time()
    state.entry_time = datetime.now()
    state.exit_time = None
    state.exit_reason = None
    state.trade_logged = False

    # if immediate option tick exists use it as entry reference
    if state.option_ltp is not None:
        state.prem_entry = state.option_ltp
    print(f"{GREEN}LIVE BUY ORDER PLACED: {symbol}{RESET}")
    print(f"{BLUE}[ORDER RESPONSE][ENTRY] {response}{RESET}")
    print(f"{BLUE}[ENTRY] Side={side} | Spot={state.spot_ltp} | Option SID={security_id}{RESET}")
    send_telegram(f"LIVE BUY ORDER PLACED\n{symbol}\nTime: {datetime.now().strftime('%H:%M:%S')}")
    return True


def log_trade_to_excel(
    symbol: str,
    side: str,
    entry_time: datetime,
    exit_time: datetime,
    entry_price: float,
    exit_price: float,
    quantity: int,
    reason: str,
) -> None:
    trade_date = entry_time.date().isoformat()
    pnl = round((exit_price - entry_price) * quantity, 2)
    row = {
        "Date": trade_date,
        "Entry Time": entry_time.strftime("%H:%M:%S"),
        "Exit Time": exit_time.strftime("%H:%M:%S"),
        "Symbol": symbol,
        "Side (CE/PE)": side,
        "Entry Price": round(entry_price, 2),
        "Exit Price": round(exit_price, 2),
        "Quantity": quantity,
        "P&L": pnl,
        "Exit Reason (SL/TARGET/DAY CLOSE)": reason,
    }
    new_df = pd.DataFrame([row])
    if os.path.exists(TRADE_LOG_FILE):
        existing_df = pd.read_excel(TRADE_LOG_FILE)
        new_df = pd.concat([existing_df, new_df], ignore_index=True)
    new_df.to_excel(TRADE_LOG_FILE, index=False, engine="openpyxl")
    print(f"{GREEN}[TRADE LOGGED] {TRADE_LOG_FILE} updated for {symbol}, P&L={pnl}{RESET}")


def place_exit(rest: DhanRestClient, reason: str) -> None:
    if not state.trade_open or not state.active_security_id or not state.active_symbol:
        return

    response = rest.place_order(security_id=state.active_security_id, side="SELL", qty=LOT_SIZE)
    if not response:
        print("[EXIT FAILED] Empty order response")
        return

    print(f"{RED}EXIT ORDER PLACED: {state.active_symbol} | Reason: {reason}{RESET}")
    if reason == "SL":
        print(f"{RED}[RISK] Stop-loss hit at option LTP={state.option_ltp}{RESET}")
    elif reason == "TARGET":
        print(f"{GREEN}[RISK] Target hit at option LTP={state.option_ltp}{RESET}")
    print(f"{BLUE}[ORDER RESPONSE][EXIT] {response}{RESET}")
    send_telegram(f"EXIT TRADE\nSymbol: {state.active_symbol}\nReason: {reason}\nTime: {datetime.now().strftime('%H:%M:%S')}")

    if (
        not state.trade_logged
        and state.entry_time
        and state.prem_entry is not None
        and state.option_ltp is not None
        and state.active_side
    ):
        state.exit_time = datetime.now()
        state.exit_reason = reason
        try:
            log_trade_to_excel(
                symbol=state.active_symbol,
                side=state.active_side,
                entry_time=state.entry_time,
                exit_time=state.exit_time,
                entry_price=state.prem_entry,
                exit_price=state.option_ltp,
                quantity=LOT_SIZE,
                reason=reason,
            )
            state.trade_logged = True
        except Exception as exc:
            print(f"{RED}[TRADE LOG ERROR] {exc}{RESET}")
            send_telegram(f"Trade log write failed: {exc}")

    state.trade_open = False
    state.day_closed = True


def _extract_ticks(response: dict) -> List[dict]:
    if not isinstance(response, dict):
        return []
    if isinstance(response.get("data"), list):
        return [x for x in response["data"] if isinstance(x, dict)]
    if isinstance(response.get("ticks"), list):
        return [x for x in response["ticks"] if isinstance(x, dict)]
    if response.get("security_id") or response.get("securityId"):
        return [response]
    return []


def handle_tick(rest: DhanRestClient, tick: dict, option_map: Dict[str, str], option_index: dict, expiry: date) -> None:
    now = datetime.now().time()

    secid = str(tick.get("security_id") or tick.get("securityId") or "")
    ltp = _to_float(tick.get("LTP") or tick.get("last_price") or tick.get("ltp"))
    if not secid or ltp is None:
        return

    if secid == SPOT_SECURITY_ID:
        state.spot_ltp = ltp
        now_ts = time.time()
        if now_ts - state.last_spot_log_ts >= 2:
            state.last_spot_log_ts = now_ts
            print(f"{BLUE}[SPOT] NIFTY LTP={state.spot_ltp}{RESET}")
    elif state.active_security_id and secid == str(state.active_security_id):
        state.option_ltp = ltp
        state.last_option_tick_ts = time.time()
        now_ts = time.time()
        if now_ts - state.last_option_log_ts >= 2:
            state.last_option_log_ts = now_ts
            print(f"{YELLOW}[OPTION] {state.active_symbol or state.active_security_id} LTP={state.option_ltp}{RESET}")

    # Forced day close
    if now >= FORCE_EXIT_TIME and not state.day_closed:
        place_exit(rest, "DAY CLOSE")
        return

    if state.day_closed or now >= LAST_ENTRY_TIME:
        return

    # Entry logic (one trade/day)
    if not state.trade_open and not state.order_placed and state.auto_ready and state.cpr_type != "WIDE":
        if not state.allowed_side:
            return

        if should_enter("CE") and state.allowed_side == "CE":
            sym, sid = get_atm_option(state.spot_ltp, "CE", option_index, expiry)
            if sym and sid:
                print(f"{GREEN}[ENTRY TRIGGER] CE breakout @ spot {state.spot_ltp}{RESET}")
                option_map[sid] = sym
                place_entry(rest, sym, sid, "CE")
        elif should_enter("PE") and state.allowed_side == "PE":
            sym, sid = get_atm_option(state.spot_ltp, "PE", option_index, expiry)
            if sym and sid:
                print(f"{GREEN}[ENTRY TRIGGER] PE breakdown @ spot {state.spot_ltp}{RESET}")
                option_map[sid] = sym
                place_entry(rest, sym, sid, "PE")

    # Management logic
    if state.trade_open and state.option_ltp is not None:
        if state.prem_entry is None:
            state.prem_entry = state.option_ltp
            state.prem_sl = round(state.prem_entry - PREM_SL_PTS, 2)
            state.prem_target = round(state.prem_entry + PREM_TGT_PTS, 2)
            print(f"Premium Entry: {state.prem_entry} | Target: {state.prem_target} | SL: {state.prem_sl}")
            print(f"{BLUE}[ORDER RESPONSE][ENTRY] Entry price reference captured from option ticks.{RESET}")
            send_telegram(
                f"Premium Entry: {state.prem_entry}\nTarget: {state.prem_target}\nSL: {state.prem_sl}"
            )
            return

        if state.prem_sl is not None and state.option_ltp <= state.prem_sl:
            place_exit(rest, "SL")
        elif state.prem_target is not None and state.option_ltp >= state.prem_target:
            place_exit(rest, "TARGET")


def run_marketfeed_loop(rest: DhanRestClient, option_index: dict, expiry: date) -> None:
    reconnect_delay = RECONNECT_BASE_DELAY
    option_map: Dict[str, str] = {}

    while not state.day_closed:
        # instrument format required by Dhan marketfeed v2
        instruments = [(marketfeed.NSE, str(SPOT_SECURITY_ID), marketfeed.Ticker)]
        if state.active_security_id:
            instruments.append((marketfeed.NSE_FNO, str(state.active_security_id), marketfeed.Ticker))

        data = None
        ws_thread = None
        try:
            data = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, "v2")
            ws_thread = threading.Thread(target=data.run_forever, daemon=True)
            ws_thread.start()

            empty_ticks = 0
            print(f"{GREEN}WebSocket connected with {len(instruments)} instrument(s){RESET}")

            while not state.day_closed:
                if rest.auth_failed:
                    print("[AUTH ERROR] Exiting due to invalid/expired token.")
                    send_telegram("[AUTH ERROR] Access token expired/invalid. Bot stopping.")
                    state.day_closed = True
                    break

                if state.feed_resubscribe_required:
                    state.feed_resubscribe_required = False
                    raise ConnectionError("Resubscribe required for newly selected option instrument")

                response = data.get_data()
                if not response:
                    empty_ticks += 1
                    if empty_ticks % EMPTY_TICK_LOG_EVERY == 0:
                        print(f"{YELLOW}No tick data ({empty_ticks} empty reads).{RESET}")

                    # REST fallback while websocket is quiet
                    if empty_ticks % int(max(1, REST_FALLBACK_INTERVAL_SEC / 0.25)) == 0:
                        spot_ltp = rest.get_ltp(SPOT_SECURITY_ID, "IDX_I")
                        if spot_ltp is not None:
                            handle_tick(
                                rest,
                                {"security_id": str(SPOT_SECURITY_ID), "ltp": spot_ltp},
                                option_map,
                                option_index,
                                expiry,
                            )
                        if state.active_security_id:
                            opt_ltp = rest.get_ltp(str(state.active_security_id), "NSE_FNO")
                            if opt_ltp is not None:
                                handle_tick(
                                    rest,
                                    {"security_id": str(state.active_security_id), "ltp": opt_ltp},
                                    option_map,
                                    option_index,
                                    expiry,
                                )

                    if empty_ticks > MAX_EMPTY_TICKS:
                        raise ConnectionError("No marketfeed data for extended duration")
                    time.sleep(0.25)
                    continue

                empty_ticks = 0
                reconnect_delay = RECONNECT_BASE_DELAY
                ticks = _extract_ticks(response)
                if not ticks:
                    continue
                for tick in ticks:
                    handle_tick(rest, tick, option_map, option_index, expiry)

        except KeyboardInterrupt:
            state.day_closed = True
            print("Interrupted by user.")
            break
        except Exception as exc:
            if state.day_closed:
                break
            state.reconnect_failures += 1
            if state.reconnect_failures > MAX_RECONNECT_ATTEMPTS:
                print(f"{RED}[FATAL] Max reconnect attempts exceeded. Stopping bot safely.{RESET}")
                send_telegram("[FATAL] Max websocket reconnect attempts exceeded. Bot stopped.")
                state.day_closed = True
                break
            print(
                f"{YELLOW}WebSocket error: {exc}. Reconnecting in {reconnect_delay}s..."
                f" (attempt {state.reconnect_failures}/{MAX_RECONNECT_ATTEMPTS}){RESET}"
            )
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
        finally:
            if data is not None:
                try:
                    data.close()
                    print(f"{YELLOW}WebSocket connection closed cleanly before reconnect.{RESET}")
                except Exception as close_exc:
                    print(f"{YELLOW}WebSocket close warning: {close_exc}{RESET}")
            if ws_thread is not None and ws_thread.is_alive():
                ws_thread.join(timeout=2)


def main() -> None:
    ensure_single_instance()
    atexit.register(remove_lock)

    if CLIENT_ID == "YOUR_CLIENT_ID" or ACCESS_TOKEN == "YOUR_ACCESS_TOKEN":
        print("Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN environment variables.")
        return

    print(f"{GREEN}MODE: OPTION BUYING SCRIPT - LIVE TRADE{RESET}")
    print(f"{BLUE}Execution Date: {date.today()} | {datetime.now().strftime('%H:%M:%S')}{RESET}")
    send_telegram("🚀 Script Started")

    # Init official client (kept for compatibility and future expansion)
    _ = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    rest = DhanRestClient(CLIENT_ID, ACCESS_TOKEN)

    print("Downloading instruments...")
    instruments = download_nfo_master_with_retry()
    if not instruments:
        print("[FATAL] Instrument master unavailable after retry. Exiting.")
        return
    option_index, next_week_expiry = build_option_index(instruments)
    if not next_week_expiry:
        print("No valid NIFTY expiry found in instrument master.")
        return
    print(f"Loaded NFO instruments. Using expiry: {next_week_expiry}")

    # wait until 09:35 for completed 09:30 candle
    while datetime.now().time() < dtime(9, 35):
        print("Waiting for 9:35 to fetch 9:30 candle...")
        time.sleep(5)

    state.candle = fetch_930_candle(rest)
    if not state.candle:
        print("Failed to fetch 9:30 candle. Exiting safely.")
        return

    print(f"{GREEN}Fetched 9:30 candle: High={state.candle.high}, Low={state.candle.low}{RESET}")
    send_telegram("Fetched 9:30 candle successfully")

    # warm-up spot LTP
    state.spot_ltp = rest.get_ltp(SPOT_SECURITY_ID, "IDX_I")
    if rest.auth_failed:
        print("[AUTH ERROR] Token invalid/expired. Exiting cleanly.")
        send_telegram("[AUTH ERROR] Token invalid/expired. Bot stopped.")
        state.day_closed = True
        sys.exit(1)
    if state.spot_ltp is None:
        print("Spot LTP unavailable at startup; will continue with websocket updates.")

    calculate_auto_signal(rest)

    if not state.auto_ready:
        print("Auto signal not ready due to missing data. Exiting.")
        return

    run_marketfeed_loop(rest, option_index, next_week_expiry)

    print("Script exited cleanly")
    send_telegram("🛑 Script Stopped")


if __name__ == "__main__":
    main()
