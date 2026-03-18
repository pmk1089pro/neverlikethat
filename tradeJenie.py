# tradeJenie.py

import sys
import time
import datetime
from datetime import timedelta
import pandas as pd
import sqlite3
import logging
from commonFunction import check_monthly_stoploss_hit, close_position_and_no_new_trade, convertIntoHeikinashi, delete_open_position, generate_god_signals, get_next_candle_time, get_optimal_option, get_trade_configs, hd_strategy, init_db, is_market_open, load_open_position, railway_track_strategy, record_trade, save_open_position, update_trade_config_on_failure, validate_trade_prices, wait_until_next_candle, who_tried, will_market_open_within_minutes,get_hedge_option,get_lot_size,check_trade_stoploss_hit,get_keywise_trade_config,is_valid_trade_data,get_clean_trade
from config import  HEDGE_NEAREST_LTP, SYMBOL,SEGMENT, CANDLE_DAYS as DAYS, REQUIRED_CANDLES, LOG_FILE,INSTRUMENTS_FILE, OPTION_SYMBOL, SERVER, ROLLOVER_CALC
from kitefunction import get_historical_df, place_option_hybrid_order, get_token_for_symbol, get_quotes_with_retry, place_robust_limit_order
from telegrambot import send_telegram_message,send_telegram_message_admin
import importlib
import threading
import pandas as pd
from requests.exceptions import ReadTimeout
from kiteconnect import exceptions
import random

# ====== Setup Logging ======
logging.basicConfig(
    filename=LOG_FILE,
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
instrument_token = get_token_for_symbol(SYMBOL)

if instrument_token is None:
    logging.error(f"❌ Instrument token for {SYMBOL} not found. Exiting.")
    exit(1)
logging.info(f"ℹ️ Instrument token for {SYMBOL}: {instrument_token} at current time {current_time}")

# ====== Main Live Trading Loconfig['REAL_TRADE']op ======
def live_trading(instruments_df, config, key, user):

    if config['REAL_TRADE'].lower() != "yes":
        print(f"🚫{key} | {user['user']} {SERVER}  | TRADE mode is OFF SIMULATED_ORDER will be tracked")
        # send_telegram_message(f"🛠️ {user['user']} {SERVER}  |  {key}  | OnlyLive {config['INTERVAL']} running in {'SIMULATION' if config['REAL_TRADE'].lower() != 'yes' else 'LIVE'} mode.",user['telegram_chat_id'], user['telegram_token'])
        logging.info(f"🚫 {key} | {user['user']} {SERVER}  | TRADE mode is OFF. Running in SIMULATION mode.")
    else:    
        print(f"🚀 {key} | {user['user']} {SERVER}  | TRADE mode is ON LIVE_ORDER will be placed")
        # send_telegram_message(f"🚀 {user['user']} {SERVER}  |  {key}  | {config['INTERVAL']} Live trading started!",user['telegram_chat_id'], user['telegram_token'])
        logging.info(f"🚀 {key} | {user['user']} {SERVER}  | TRADE mode is ON. Running in LIVE mode.")
    
    open_trade = load_open_position(config, key, user, user['id'])
    if open_trade:
            trade = open_trade
            position = open_trade["Signal"]
            logging.info(f"📌 {key}  |   {user['user']} {SERVER}  | Resumed open position: {position} | {open_trade['OptionSymbol']} @ ₹{open_trade['OptionSellPrice']} | Qty: {open_trade['qty']} | Hedge Symbol: {open_trade['hedge_option_symbol']} @ ₹{open_trade['hedge_option_buy_price']} | Hedge Qty: {open_trade['hedge_qty']}")
            print(f"📌 {key}  |   {user['user']} {SERVER}  | Resumed open position: {position} | {open_trade['OptionSymbol']} @ ₹{open_trade['OptionSellPrice']} | Qty: {open_trade['qty']} | Hedge Symbol: {open_trade['hedge_option_symbol']} @ ₹{open_trade['hedge_option_buy_price']} | Hedge Qty: {open_trade['hedge_qty']}")
            send_telegram_message(f"📌 {key} | {user['user']} {SERVER}  |  {config['INTERVAL']} Resumed open position: {position} | {open_trade['OptionSymbol']} @ ₹{open_trade['OptionSellPrice']} | Qty: {open_trade['qty']} | Hedge Symbol: {open_trade['hedge_option_symbol']} @ ₹{open_trade['hedge_option_buy_price']} | Hedge Qty: {open_trade['hedge_qty']}",user['telegram_chat_id'], user['telegram_token'])
    else:
        trade = {}
        position = None
        print(f"ℹ️ {key} | {user['user']} {SERVER}  |  {config['INTERVAL']} No open position. Waiting for next signal...")
        logging.info(f"ℹ️ {key} | {user['user']} {SERVER}  |  {config['INTERVAL']} No open position. Waiting for next signal...")
   
    

    while True:
        open_trade = load_open_position(config, key, user, user['id'])
        if open_trade:
            trade = open_trade
            position = open_trade["Signal"]
            logging.info(f"📌 {key}  | {user['user']} {SERVER}  |Resumed open position: {position} | {open_trade['OptionSymbol']} @ ₹{open_trade['OptionSellPrice']} | Qty: {open_trade['qty']} | Hedge Symbol: {open_trade['hedge_option_symbol']} @ ₹{open_trade['hedge_option_buy_price']} | Hedge Qty: {open_trade['hedge_qty']}")
            print(f"➡️ {key}  |  {user['user']} {SERVER} Loaded open position: {open_trade}")
            # send_telegram_message(f"📌 {key}  | {user['user']} {SERVER}  |  {config['INTERVAL']} Resumed open position: {position} | {open_trade['OptionSymbol']} @ ₹{open_trade['OptionSellPrice']} | Qty: {open_trade['qty']} | Hedge Symbol: {open_trade['hedge_option_symbol']} @ ₹{open_trade['hedge_option_buy_price']} | Hedge Qty: {open_trade['hedge_qty']}",user['telegram_chat_id'], user['telegram_token'])
        else:
            trade = {}
            position = None
            print(f"ℹ️ {key} | {user['user']} {SERVER} No open position. Waiting for next signal...")
            logging.info(f"ℹ️ {key} | {user['user']} {SERVER} No open position. Waiting for next signal...")
   
        try:
            # configs = get_trade_configs(user['id'])
            config = get_keywise_trade_config(key)
            lot_size = get_lot_size(config, instruments_df)
            config['QTY'] = lot_size*int(config['LOT'])

            if config['NEW_TRADE'].lower() == "no" and trade == {}:   
                print(f"🚫 {key}  | {user['user']} {SERVER}, There is no live trade present, No new trades allowed. So Closing the program")
                logging.info(f"🚫 {key}  |{user['user']} {SERVER}, There is no live trade present, No new trades allowed. So Closing the program")
                send_telegram_message(f"🕒 {key}  | {user['user']} {SERVER}, There is no live trade present, No new trades allowed. So Closing the program",user['telegram_chat_id'], user['telegram_token'])
                break    
            
            

            if not is_market_open():
                print(f" {key}  | {user['user']} {SERVER} Market is closed. Checking if market will open within 60 minutes...")
                if will_market_open_within_minutes(60):
                    print(f" {key} | {user['user']} {SERVER}Market will open within 60 minutes. Continuing to wait...")
                    time.sleep(60)
                    continue
                else:
                    print(f"{key}  | {user['user']} {SERVER}, Market will not open within 60 minutes. Stopping program.")
                    send_telegram_message(f"🛑 {key}  | {user['user']} {SERVER}, Market will not open within 60 minutes. Stopping program.",user['telegram_chat_id'], user['telegram_token'])
                    return

            if config['INTRADAY'].lower() == "yes" and trade == {} and datetime.datetime.now().time() >= datetime.time(15, 15):
                print(f"🚫 {key}  | {user['user']} {SERVER}, There is no live trade present, No new trades allowed. So Closing the program")
                logging.info(f"🚫{key}  |{user['user']} {SERVER}, There is no live trade present, No new trades allowed. So Closing the program")
                send_telegram_message(f"🕒 {key}  | {user['user']} {SERVER}, There is no live trade present, No new trades allowed. So Closing the program",user['telegram_chat_id'], user['telegram_token'])
                break     

            df = get_historical_df(instrument_token, config['INTERVAL'], DAYS, user)
            print(f"🕵️‍♀️{key} | {user['user']} {SERVER} Candles available: {len(df)} / Required: {REQUIRED_CANDLES}")

            if len(df) < REQUIRED_CANDLES:
                print(f"⚠️ {key}  | {user['user']} {SERVER} Not enough candles. Waiting...")
                logging.warning(f"⚠️ {key}  | {user['user']} {SERVER} Not enough candles. Waiting...")
                time.sleep(60)
                continue
            
            if config['STRATEGY'] == "GOD":
                df = generate_god_signals(df)
            elif config['STRATEGY'] == "HDSTRATEGY":
                df = convertIntoHeikinashi(df)
                df = hd_strategy(df)
            elif config['STRATEGY'] == "RAILWAY_TRACK":
                df = railway_track_strategy(df)
            
            latest = df.iloc[-1]
            latest_time = pd.to_datetime(latest['date'])
            # now = datetime.now()

            # ✅ Decide which row to use for signals
            if df.iloc[-1]['buySignal'] or df.iloc[-1]['sellSignal']:
                latest = df.iloc[-1]
            elif df.iloc[-2]['buySignal'] or df.iloc[-2]['sellSignal']:
                latest = df.iloc[-2]
            else:
                latest = df.iloc[-1]  # No signal in last 2 candles

            ts = latest['date'].strftime('%Y-%m-%d %H:%M')
            close = latest['close']
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f"🕒{key} | Signal Received at Current Time: {current_time}\n{df.tail(5)}")            
            logging.info(f"{key} | {config['STRATEGY']} | Candle time {ts} | Close: {close} | Buy: {latest['buySignal']} | Sell: {latest['sellSignal']} | Trend: {latest['trend']} | Current Time: {current_time}")
            print(f"{key} | {config['STRATEGY']} | Candle time {ts} | Close: {close} | Buy: {latest['buySignal']} | Sell: {latest['sellSignal']} | Trend: {latest['trend']} | Current Time: {current_time}")
            
            
            if config['HEDGE_TYPE'] != "NH":
                # ✅ BUY SIGNAL
                if latest['buySignal'] and position != "BUY":
                    if position == "SELL":
                        # EXIT CODE EXECUTION :: START
                        # 1. PRE-EXIT PREPARATION
                        # Extract existing quantities and settings
                        existing_qty = int(trade.get("qty", config['QTY']))
                        target_qty_new = int(config.get('QTY', existing_qty))
                        hr_type = str(config.get('HEDGE_ROLLOVER_TYPE', 'FULL')).upper()
                        

                        print(f"📥 {key} |  {user['user']} {SERVER} | Exiting  {trade['OptionSymbol']} | {trade['hedge_option_symbol']} | Qty: {existing_qty}" )
                        logging.info(f"📥 {key} |  {user['user']} {SERVER} | Exiting  {trade['OptionSymbol']} | {trade['hedge_option_symbol']} | Qty: {existing_qty}" )

                        # 2. EXECUTE ROBUST EXIT (Replaces manual hybrid orders)
                        # This function handles NH/SEMI/FULL and Qty Changes internally.
                        # It will KILL the thread if a mismatch or partial fill occurs.
                        exit_qty, avg_price, hedge_avg_price = execute_robust_exit( trade, config, user)
                        
                        logging.info(f"📤{key} | Exited from {trade['OptionSymbol']} with Avg price: ₹{avg_price:.2f} | Qty: {exit_qty} And {trade['hedge_option_symbol']} with Avg price: ₹{hedge_avg_price:.2f} | Qty: {exit_qty}" )

                        if not is_valid_trade_data(exit_qty, avg_price, hedge_avg_price, hedge_required=True):
                            err_msg = f"⚠️ {key} | FAILED EXIT: Qty ({exit_qty}) or Price ({avg_price}) or ({hedge_avg_price}) is 0. Database NOT updated."
                            logging.error(err_msg)
                            send_telegram_message_admin(err_msg)
                            update_trade_config_on_failure(config['KEY'], err_msg, user)
                            return 

                        # 3. UPDATE DATA & RECORD
                        trade.update({
                            "OptionBuyPrice": avg_price,
                            "SpotExit": close,
                            "ExitTime": current_time,
                            "PnL": trade["OptionSellPrice"] - avg_price,
                            "qty": exit_qty,
                            "ExitReason": "SIGNAL_GENERATED",
                            "hedge_option_sell_price": hedge_avg_price,
                            "hedge_exit_time": current_time,
                            "hedge_pnl": (hedge_avg_price - trade["hedge_option_buy_price"]) if hedge_avg_price > 0 else 0,
                        })
                        
                        # Calculate Total PnL (Main + Hedge)
                        trade["total_pnl"] = trade["PnL"] + trade.get("hedge_pnl", 0)

                        logging.info(f"📥 {key} | SELL Signal Generated | Exit Successful | Main Avg: {avg_price} | Hedge Avg: {hedge_avg_price} | Qty: {exit_qty}")
                        
                        trade = get_clean_trade(trade)
                        record_trade(trade, config, user['id'])
                        delete_open_position(trade["OptionSymbol"], config, trade, user['id'])
                        
                        # 4. NOTIFY
                        msg = (f"📤{key} | {user['user']} {SERVER} | Exit SELL Signal Generated.\n"
                            f"{trade['OptionSymbol']} @ ₹{avg_price:.2f}\n"
                            f"Hedge: {trade['hedge_option_symbol']} @ ₹{hedge_avg_price:.2f}\n"
                            f"Profit/Qty: ₹{trade['total_pnl']:.2f}")
                        send_telegram_message(msg, user['telegram_chat_id'], user['telegram_token'])
                    # EXIT CODE EXECUTION :: END
                    if config['NEW_TRADE'].lower() == "no":
                        print(f"🚫{key} | {user['user']} | {SERVER} | No new trades allowed. Skipping BUY signal.")
                        logging.info(f"{key} | {user['user']} | {SERVER} | No new trades allowed. Skipping BUY signal.")
                        break

                    
                    if check_monthly_stoploss_hit(user, config):
                        break

                    # ENTRY CODE EXECUTION :: START
                    result = (None, None, None, None)
                    for attempt in range(3):
                        result = get_optimal_option("BUY", close, config['NEAREST_LTP'], instruments_df, config, user)
                        
                        # If the function returned a valid symbol (not None), we are done!
                        if result[0] is not None:
                            break
                            
                        logging.info(f"⚠️{key}  |  {user['user']}  |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                        time.sleep(2)
                        
                    strike = result[1]
                    main_ltp = result[3]
                    if(config['HEDGE_TYPE'] == "H-P10" ):
                        hedge_result = (None, None, None, None)
                        for attempt in range(3):
                            hedge_result = get_optimal_option("BUY", close, HEDGE_NEAREST_LTP, instruments_df, config, user)
                            
                            # If the function returned a valid symbol (not None), we are done!
                            if hedge_result[0] is not None:
                                break
                                
                            logging.info(f"⚠️{key}  |  {user['user']}  |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                            time.sleep(2)
                            
                    elif(config['HEDGE_TYPE'] == "H-M100" or config['HEDGE_TYPE'] == "H-M200" ):
                        hedge_result = (None, None, None, None)
                        if strike != None :
                            for attempt in range(3):
                                # Try to find the option
                                hedge_result = get_hedge_option("BUY", close, strike, main_ltp, instruments_df, config, user)
                                
                                # If the function returned a valid symbol (not None), we are done!
                                if hedge_result[0] is not None:
                                    break
                                    
                                print(f"⚠️{key}  |  {user['user']}  |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                                logging.info(f"⚠️{key}  |  {user['user']}  |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                                time.sleep(2)

                            

                    if result is None or result[0] is None or (config['HEDGE_TYPE'] != "NH" and (hedge_result is None or hedge_result[0] is None)):
                        logging.error(f"❌{key} | INTERVAL {config['INTERVAL']} | {user['user']} {SERVER}  |  : No suitable option found for BUY signal.")
                        send_telegram_message(f"❌{key} | INTERVAL {config['INTERVAL']} | {user['user']} {SERVER}  |  : No suitable option found for BUY signal.",user['telegram_chat_id'], user['telegram_token'])
                        err_msg = ""
                        if result is None or result[0] is None:
                            err_msg = f"❌{key} | INTERVAL {config['INTERVAL']} | {user['user']} {SERVER}  |  : No suitable Main option found for BUY signal."
                        else:
                            err_msg = f"❌{key} | INTERVAL {config['INTERVAL']} | {user['user']} {SERVER}  |  : No suitable Hedge option found for BUY signal."
                        send_telegram_message_admin(err_msg)
                        continue
                    else:
                        opt_symbol, strike, expiry, ltp = result
                        hedge_opt_symbol, hedge_strike, hedge_expiry, hedge_ltp = hedge_result if hedge_result else (None, None, None, 0)

                        # Create temporary trade dict for the entry function to use symbols
                        temp_trade_symbols = {
                            "OptionSymbol": opt_symbol,
                            "hedge_option_symbol": hedge_opt_symbol
                        }

                        print(f"📤{key} | {user['user']} {SERVER}  | Entering Entry Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")
                        logging.info(f"📤 {key} | Entering Entry Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")

                        # --- ROBUST ENTRY EXECUTION ---
                        # Replaces place_option_hybrid_order calls and None-checks.
                        # Handles Matched Partials, Mismatch Reversals, and Kills Thread on failure.
                        qty, avg_price, hedge_avg_price = execute_robust_entry(temp_trade_symbols, config, user)

                        logging.info(f"📤{key} | Entered in {opt_symbol} with Avg price: ₹{avg_price:.2f} | Qty: {qty}. And  {hedge_opt_symbol} with Avg price: ₹{hedge_avg_price:.2f} | Qty: {qty}")
                        
                        if not is_valid_trade_data(qty, avg_price, hedge_avg_price, hedge_required=True):
                            err_msg = f"⚠️ {key} | FAILED Entry: Qty ({qty}) or Price ({avg_price}) or ({hedge_avg_price}) is 0. Database NOT updated."
                            logging.error(err_msg)
                            send_telegram_message_admin(err_msg)
                            break 
                        
                        trade = {
                            "Signal": "BUY", "SpotEntry": close, "OptionSymbol": opt_symbol,
                            "Strike": strike, "Expiry": expiry,
                            "OptionSellPrice": avg_price, "EntryTime": current_time,
                            "qty": qty, "interval": config['INTERVAL'], "real_trade": config['REAL_TRADE'],
                            "EntryReason":"SIGNAL_GENERATED", "ExpiryType":config['EXPIRY'],
                            "Strategy":config['STRATEGY'], "Key":key, "hedge_option_symbol":hedge_opt_symbol,
                            "hedge_strike":hedge_strike, "hedge_option_buy_price":hedge_avg_price,
                            "hedge_qty": qty if config['HEDGE_TYPE'] != "NH" else 0, 
                            "hedge_entry_time": current_time
                        }
                        
                        trade = get_clean_trade(trade)
                        save_open_position(trade, config, user['id'])
                        position = "BUY"
                        send_telegram_message(f"🟢{key} | {user['user']} {SERVER}  |  Buy Signal\n{opt_symbol} | Avg ₹{avg_price:.2f} | Qty: {qty}. Hedge Symbol {trade['hedge_option_symbol']} | @ ₹{trade['hedge_option_buy_price']:.2f}", user['telegram_chat_id'], user['telegram_token'])
                        # ENTRY CODE EXECUTION :: END
                # ✅ SELL SIGNAL
                elif latest['sellSignal'] and position != "SELL":
                    
                    if position == "BUY":
                        # EXIT CODE EXECUTION :: START
                        existing_qty = int(trade.get("qty", config['QTY']))
                        
                        print(f"📥 {key} |  {user['user']} {SERVER} | Signal Exit: Buying back {trade['OptionSymbol']} | Qty: {existing_qty} | Hedge: {trade['hedge_option_symbol']}")
                        logging.info(f"📥  {key} |  {user['user']} {SERVER} | Signal Exit: Buying back {trade['OptionSymbol']} | Qty: {existing_qty} | Hedge: {trade['hedge_option_symbol']}")
    
                        # 2. ROBUST EXIT EXECUTION
                        # Handles SEMI/FULL/NH and Qty Changes. Kills thread if mismatch occurs.
                        exit_qty, avg_price, hedge_avg_price = execute_robust_exit(
                            trade, 
                            config, 
                            user
                        )

                        logging.info(f"📤{key} | Exited from {trade['OptionSymbol']} with Avg price: ₹{avg_price:.2f} | Qty: {exit_qty} And {trade['hedge_option_symbol']} with Avg price: ₹{hedge_avg_price:.2f} | Qty: {exit_qty}" )

                        if not is_valid_trade_data(exit_qty, avg_price, hedge_avg_price, hedge_required=True):
                            err_msg = f"⚠️ {key} | FAILED EXIT: Qty ({exit_qty}) or Price ({avg_price}) or ({hedge_avg_price}) is 0. Database NOT updated."
                            logging.error(err_msg)
                            send_telegram_message_admin(err_msg)
                            update_trade_config_on_failure(config['KEY'], err_msg, user)
                            return 
                        
                        # 3. UPDATE DATA & CALCULATE PNL
                        trade.update({
                            "OptionBuyPrice": avg_price,
                            "SpotExit": close,
                            "ExitTime": current_time,
                            "PnL": trade["OptionSellPrice"] - avg_price,
                            "qty": exit_qty,
                            "ExitReason": "SIGNAL_GENERATED",
                            "hedge_option_sell_price": hedge_avg_price,
                            "hedge_exit_time": current_time,
                            "hedge_pnl": (hedge_avg_price - trade["hedge_option_buy_price"]) if hedge_avg_price > 0 else 0,
                        })
                        
                        # Calculate Total PnL (Main + Hedge)
                        trade["total_pnl"] = trade["PnL"] + trade.get("hedge_pnl", 0)

                        logging.info(f"📥 {key} |{user['user']} {SERVER} | Signal Exit Success | {trade['OptionSymbol']} of M_Avg: {avg_price} | {trade['hedge_option_symbol']} of H_Avg: {hedge_avg_price}")
                        
                        trade = get_clean_trade(trade)
                        record_trade(trade, config, user['id'])
                        delete_open_position(trade["OptionSymbol"], config, trade, user['id'])
                        
                        # 4. NOTIFY
                        msg = (f"📤{key} | {user['user']} {SERVER} |  Exit Signal Generated\n"
                            f"{trade['OptionSymbol']} @ ₹{avg_price:.2f}\n"
                            f"Hedge: {trade['hedge_option_symbol']} @ ₹{hedge_avg_price:.2f}\n"
                            f"Total PnL/Qty: ₹{trade['total_pnl']:.2f}")
                        
                        logging.info(msg)
                        send_telegram_message(msg, user['telegram_chat_id'], user['telegram_token']) 
                    # EXIT CODE EXECUTION :: END                   
                    
                    if config['NEW_TRADE'].lower() == "no":
                        print(f"🚫  {key}  | {user['user']} {SERVER}  |  No new trades allowed. Skipping SELL signal.")
                        logging.info(f"🚫 {key} | {user['user']} {SERVER}  | No new trades allowed. Skipping SELL signal.")
                        break
                    
                    if check_monthly_stoploss_hit(user, config):
                        break
                    
                           
                    # ENTRY CODE EXECUTION :: START
                    result = (None, None, None, None)

                    for attempt in range(3):
                        result = get_optimal_option("SELL", close, config['NEAREST_LTP'], instruments_df, config, user)
                        
                        # If the function returned a valid symbol (not None), we are done!
                        if result[0] is not None:
                            break
                            
                        logging.info(f"⚠️{key}  |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                        time.sleep(2)
                        
                    print(f"📤 {key}  | {user['user']} {SERVER} | Signal Generated for Entry : Optimal option search completed with result: {result}")
                    logging.info(f"📤 {key} | {user['user']} {SERVER} | Signal Generated for Entry : Optimal option search completed with result: {result}")
                    strike = result[1]
                    main_ltp = result[3]
                    if(config['HEDGE_TYPE'] == "H-P10" ):
                        
                        hedge_result = (None, None, None, None)
                        for attempt in range(3):
                            hedge_result = get_optimal_option("SELL", close, HEDGE_NEAREST_LTP, instruments_df, config, user)
                            
                            # If the function returned a valid symbol (not None), we are done!
                            if hedge_result[0] is not None:
                                break
                                
                            logging.info(f"⚠️{key} |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                            time.sleep(2)
                            
                    elif(config['HEDGE_TYPE'] == "H-M100" or config['HEDGE_TYPE'] == "H-M200" ):
                        
                        hedge_result = (None, None, None, None)
                        if strike != None :
                            for attempt in range(3):
                                # Try to find the option
                                hedge_result = get_hedge_option("SELL", close, strike, main_ltp, instruments_df, config, user)
                                
                                # If the function returned a valid symbol (not None), we are done!
                                if hedge_result[0] is not None:
                                    break
                                    
                                print(f"⚠️{key} | {user['user']} Search Attempt {attempt+1} failed to find an Hedge option. Retrying in 2s...")
                                logging.info(f"⚠️{key} | {user['user']} Search Attempt {attempt+1} failed to find an Hedge option. Retrying in 2s...")
                                time.sleep(2)

                    if result is None or result[0] is None or (config['HEDGE_TYPE'] != "NH" and (hedge_result is None or hedge_result[0] is None)):
                        logging.error(f"❌{config['KEY']} | {SERVER}: No suitable option found for SELL signal.")
                        send_telegram_message(f"❌{config['KEY']} | {SERVER}: No suitable option found for SELL signal.",user['telegram_chat_id'], user['telegram_token'])
                        err_msg = ""
                        if result is None or result[0] is None:
                            err_msg = f"❌{config['KEY']} | {SERVER} : No suitable Main option found for SELL signal."
                        else:
                            err_msg = f"❌{config['KEY']} | {SERVER}: No suitable Hedge option found for SELL signal."
                        send_telegram_message_admin(err_msg)
                        continue
                    else:
                        opt_symbol, strike, expiry, ltp = result
                        hedge_opt_symbol, hedge_strike, hedge_expiry, hedge_ltp = hedge_result if hedge_result else (None, None, None, 0)

                        # Create temporary trade symbols for the entry function
                        temp_trade_symbols = {
                            "OptionSymbol": opt_symbol,
                            "hedge_option_symbol": hedge_opt_symbol
                        }

                        print(f"📤{key} | Entering SELL Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")
                        logging.info(f"📤 {key} | Entering SELL Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")

                        # --- ROBUST ENTRY EXECUTION ---
                        # Handles Matched Partials (10=10), Mismatch Reversals, and Kills Thread on failure.
                        qty, avg_price, hedge_avg_price = execute_robust_entry(temp_trade_symbols, config, user)
                        # ------------------------------

                        logging.info(f"📤{key} | Entered in {opt_symbol} with Avg price: ₹{avg_price:.2f} | Qty: {qty}. And  {hedge_opt_symbol} with Avg price: ₹{hedge_avg_price:.2f} | Qty: {qty}")
                        
                        if not is_valid_trade_data(qty, avg_price, hedge_avg_price, hedge_required=True):
                            err_msg = f"⚠️ {key} | FAILED Entry: Qty ({qty}) or Price ({avg_price}) or ({hedge_avg_price}) is 0. Database NOT updated."
                            logging.error(err_msg)
                            send_telegram_message_admin(err_msg)
                            break

                        trade = {
                            "Signal": "SELL", "SpotEntry": close, "OptionSymbol": opt_symbol,
                            "Strike": strike, "Expiry": expiry,
                            "OptionSellPrice": avg_price, "EntryTime": current_time,
                            "qty": qty,  "interval": config['INTERVAL'], "real_trade": config['REAL_TRADE'],
                            "EntryReason":"SIGNAL_GENERATED", "ExpiryType":config['EXPIRY'],
                            "Strategy":config['STRATEGY'], "Key":key,
                            "hedge_option_symbol":hedge_opt_symbol,
                            "hedge_strike":hedge_strike, "hedge_option_buy_price":hedge_avg_price,
                            "hedge_qty": qty if config['HEDGE_TYPE'] != "NH" else 0, 
                            "hedge_entry_time": current_time
                        }
                        
                        trade = get_clean_trade(trade)
                        save_open_position(trade, config, user['id'])
                        position = "SELL"
                        msg = f"🔴  {key} | {user['user']} {SERVER} | Signal Generated  | Sell Signal {opt_symbol} | Avg ₹{avg_price:.2f} | Qty: {qty}. Hedge Symbol {trade['hedge_option_symbol']} | @ ₹{trade['hedge_option_buy_price']:.2f}"
                        logging.info(msg)
                        send_telegram_message(msg,user['telegram_chat_id'], user['telegram_token'])
                    # ENTRY CODE EXECUTION :: END

                next_candle_time = get_next_candle_time(config['INTERVAL'])
                # ✅ Add this flag before the while loop
                target_hit = False
                while datetime.datetime.now() < next_candle_time:
                    # Actively monitor current position LTP
                    if trade and "OptionSymbol" in trade:
                        current_ltp = get_quotes_with_retry(trade["OptionSymbol"] ,user)
                        entry_ltp = trade["OptionSellPrice"]
                        if current_ltp != None and entry_ltp != None:
                            yestime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            percent_change = round(((current_ltp - entry_ltp) / entry_ltp) * 100,2)
                            print(f"{key} | {user['user']} | position at {yestime}: {trade['Signal']} | {trade['OptionSymbol']} | Entry LTP: ₹{entry_ltp:.2f} | Current LTP: ₹{current_ltp:.2f} | Chg % {percent_change} | Qty: {trade['qty']}")
                    
                    # ✅ Intraday  EXIT 
                    now = datetime.datetime.now()
                    
                    if now.time().hour == 15 and now.time().minute >= 15 and trade and "OptionSymbol" in trade and position:
                        if config['INTRADAY'] == "yes":
                            trade, position = close_position_and_no_new_trade(trade, position, close, ts,config, user, key)
                            print(f"⏰ {key}  | {user['user']} {SERVER}  | Intraday mode: No new trades after 3:15 PM. Waiting for market close.")
                            logging.info(f"⏰ {key}  | {user['user']} {SERVER}  | Intraday mode: No new trades after 3:15 PM. Waiting for market close.")
                            send_telegram_message(f"⏰ {key}  | {user['user']} {SERVER}  | Intraday mode: No new trades after 3:15 PM. Waiting for market close.",user['telegram_chat_id'], user['telegram_token'])
                            break

                    if trade and "OptionSymbol" in trade and "OptionSellPrice" in trade and target_hit == False:
                        current_ltp = get_quotes_with_retry(trade["OptionSymbol"] ,user)
                        entry_ltp = trade["OptionSellPrice"]

                       
                        if check_trade_stoploss_hit(user, trade, config):
                            print(f"📥{key} | {user['user']} {SERVER} |  StopLoss Exit: Buying back {trade['OptionSymbol']} | Qty: {trade['qty']} and {trade['hedge_option_symbol']}")
                            logging.info(f"📥{key} | {user['user']} {SERVER} |  StopLoss Exit: Buying back {trade['OptionSymbol']} | Qty: {trade['qty']} and {trade['hedge_option_symbol']}")

                            # --- ROBUST EXIT EXECUTION ---
                            # We use DIFF to ensure BOTH legs exit on StopLoss
                            # Function will KILL thread if any mismatch or partial fill occurs
                            exit_qty, avg_price, hedge_avg_price = execute_robust_exit(
                                trade, 
                                config, 
                                user, 
                                expiry_match="DIFF" 
                            )

                            logging.info(f"📤{key} | Exited from {trade['OptionSymbol']} with Avg price: ₹{avg_price:.2f} | Qty: {exit_qty} And {trade['hedge_option_symbol']} with Avg price: ₹{hedge_avg_price:.2f} | Qty: {exit_qty}" )

                            if not is_valid_trade_data(exit_qty, avg_price, hedge_avg_price, hedge_required=True):
                                err_msg = f"⚠️ {key} | FAILED EXIT: Qty ({exit_qty}) or Price ({avg_price}) or ({hedge_avg_price}) is 0. Database NOT updated."
                                logging.error(err_msg)
                                send_telegram_message_admin(err_msg)
                                update_trade_config_on_failure(config['KEY'], err_msg, user)
                                return 

                            trade.update({
                                "OptionBuyPrice": avg_price,
                                "SpotExit": close,
                                "ExitTime": current_time,
                                "PnL": trade["OptionSellPrice"] - avg_price,
                                "qty": exit_qty,
                                "ExitReason": "STOPLOSS_HIT",
                                "hedge_option_sell_price": hedge_avg_price,
                                "hedge_exit_time": current_time,
                                "hedge_pnl": (hedge_avg_price - trade["hedge_option_buy_price"]) if hedge_avg_price > 0 else 0,
                            })
                            
                            # Final PnL Calculation
                            trade["total_pnl"] = trade["PnL"] + trade.get("hedge_pnl", 0)

                            trade = get_clean_trade(trade)
                            record_trade(trade, config, user['id'])
                            delete_open_position(trade["OptionSymbol"], config, trade, user['id'])
                            
                            # NOTIFY
                            msg = (f"📤 {user['user']} {SERVER} | {key} | StopLoss Exit {trade['Signal']}\n"
                                f"{trade['OptionSymbol']} @ ₹{avg_price:.2f}\n"
                                f"Hedge: {trade['hedge_option_symbol']} @ ₹{hedge_avg_price:.2f}\n"
                                f"Total PnL/Qty: ₹{trade['total_pnl']:.2f}")
                            send_telegram_message(msg, user['telegram_chat_id'], user['telegram_token'])
                            
                            logging.info(msg)

                            last_expiry = trade["Expiry"]
                            signal = trade["Signal"]
                            trade = {} 
                            position = None
                            break # Break out of the monitoring loop as position is closed
                        
                        # Target completed of 40%. Hence exiting current positions for Rollover.
                        if current_ltp != None and entry_ltp != None and entry_ltp != 0.0 and current_ltp <= ROLLOVER_CALC * entry_ltp:
                            
                            # --- TARGET EXIT PREPARATION ---
                            # Rollover position :: Started

                            # --- POST-EXIT RESTRICTION CHECK ---
                            if config['NEW_TRADE'].lower() == "no":
                                target_hit = True  # Set the flag to True to avoid multiple triggers
                                
                                print(f"📥 {key} | {SERVER} | Current {trade['OptionSymbol']} of Qty {trade['qty']} hit the target of 40%. Hence exiting position for Rollover.")
                                logging.info(f"📥{key} | {user['user']} {SERVER} |  Rollover Exit: Buying back {trade['OptionSymbol']} | Qty: {trade['qty']} | Selling : {trade['hedge_option_symbol']}")

                                # --- ROBUST EXIT EXECUTION ---
                                # Using DIFF ensures BOTH legs exit on Target Hit (Terminal Event)
                                # This handles NH/SEMI/FULL internally and kills thread on mismatch
                                exit_qty, avg_price, hedge_avg_price = execute_robust_exit(
                                    trade, 
                                    config, 
                                    user, 
                                    expiry_match="DIFF"
                                )

                                logging.info(f"📤{key} | Exited from {trade['OptionSymbol']} with Avg price: ₹{avg_price:.2f} | Qty: {exit_qty} And {trade['hedge_option_symbol']} with Avg price: ₹{hedge_avg_price:.2f} | Qty: {exit_qty}" )

                                if not is_valid_trade_data(exit_qty, avg_price, hedge_avg_price, hedge_required=True):
                                    err_msg = f"⚠️ {key} | FAILED EXIT: Qty ({exit_qty}) or Price ({avg_price}) or ({hedge_avg_price}) is 0. Database NOT updated."
                                    logging.error(err_msg)
                                    send_telegram_message_admin(err_msg)
                                    update_trade_config_on_failure(config['KEY'], err_msg, user)
                                    return 

                                # UPDATE TRADE DATA
                                trade.update({
                                    "OptionBuyPrice": avg_price,
                                    "SpotExit": close,
                                    "ExitTime": current_time,
                                    "PnL": trade["OptionSellPrice"] - avg_price,
                                    "qty": exit_qty,
                                    "ExitReason": "TARGET_HIT",
                                    "hedge_option_sell_price": hedge_avg_price,
                                    "hedge_exit_time": current_time,
                                    "hedge_pnl": (hedge_avg_price - trade["hedge_option_buy_price"]) if hedge_avg_price > 0 else 0,
                                })

                                # Calculate Total PnL (Main + Hedge)
                                trade["total_pnl"] = trade["PnL"] + trade.get("hedge_pnl", 0)

                                # DATABASE ACTIONS
                                trade = get_clean_trade(trade)
                                record_trade(trade, config, user['id'])
                                delete_open_position(trade["OptionSymbol"], config, trade, user['id'])

                                # NOTIFICATIONS
                                send_telegram_message(
                                    f"📤 {key}  | {user['user']} {SERVER}  |  Rollover Exit {trade['Signal']}\n"
                                    f"{trade['OptionSymbol']} @ ₹{avg_price:.2f}. "
                                    f"Hedge Symbol {trade['hedge_option_symbol']} | @ ₹{hedge_avg_price:.2f} | "
                                    f"Total Profit/Qty: {trade['total_pnl']:.2f}",
                                    user['telegram_chat_id'], user['telegram_token']
                                )

                                logging.info(f"🔴 {key} | {user['user']} {SERVER} | Target finalized for {trade['OptionSymbol']} at ₹{avg_price:.2f} | Hedge Symbol {trade['hedge_option_symbol']} | @ ₹{hedge_avg_price:.2f} | Total Profit/Qty: {trade['total_pnl']:.2f}")

                                # CLEANUP
                                last_expiry = trade["Expiry"]
                                signal = trade["Signal"]
                                trade = {} # Reset trade object

                                # NOTE: Hedge was already closed by execute_robust_exit(expiry_match="DIFF")
                                # No manual order needed here.

                                print(f"🚫  {key}  | {user['user']} {SERVER}  |  No new trades allowed after target exit.")
                                logging.info(f"🚫 {key} | {user['user']} {SERVER}  |  No new trades allowed after target exit.")
                                send_telegram_message(f"🚫 {key} | {user['user']} {SERVER}  |  No new trades allowed after target exit.", user['telegram_chat_id'], user['telegram_token'])

                                # Reset position state
                                position = None

                                # Break out of the monitoring loop
                                break
                                
                            else:
                            # --- REENTRY / ROLLOVER LOGIC ---
                                result = (None, None, None, None)
                                signal = trade["Signal"]
                                for attempt in range(3):
                                    result = get_optimal_option(signal, close, config['NEAREST_LTP'], instruments_df, config, user)
                                    
                                    # If the function returned a valid symbol (not None), we are done!
                                    if result[0] is not None:
                                        break
                                        
                                    logging.info(f"⚠️{key}  | Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                                    time.sleep(2)
                                    
                                last_expiry = trade['Expiry']
                                if result is None or result[0] is None:
                                    logging.error(f"❌ {key} | No expiry found after {last_expiry} for reentry.")
                                    position = None
                                    continue
                                else:
                                    opt_symbol, strike, expiry, ltp = result
                                    main_ltp = result[3]
                                    
                                    # 1. Determine Expiry Match
                                    # Comparing new strike expiry vs the last one we held
                                    expiry_match = "SAME" if expiry == last_expiry else "DIFF"
                                    
                                    # 2. Setup Symbols for Entry
                                    # If it's a DIFF expiry or Qty changed, we'll need a new hedge strike too
                                    target_qty = int(config['QTY'])
                                    existing_qty = int(trade.get('qty', target_qty))
                                    qty_changed = (target_qty != existing_qty)

                                    hedge_opt_symbol = trade.get('hedge_option_symbol')
                                    hedge_strike = trade.get('hedge_strike')
                                    hedge_expiry = trade.get('expiry')
                                    hedge_ltp = get_quotes_with_retry(trade.get('hedge_option_symbol') , user)

                                    # 3. EXECUTE ROBUST EXIT (Clears 130)
                                    # Automatically handles SEMI/FULL/NH and Qty Changes
                                    exit_qty, exit_avg, exit_h_avg = execute_robust_exit(trade, config, user, expiry_match=expiry_match)
                                    if expiry_match == "SAME" and not qty_changed:
                                        exit_h_avg = hedge_ltp

                                    
                                    if exit_qty > 0 and exit_avg > 0 and exit_h_avg > 0: 
                                        # UPDATE TRADE DATA
                                        trade.update({
                                            "OptionBuyPrice": exit_avg if exit_avg > 0 else 0,
                                            "SpotExit": close,
                                            "ExitTime": current_time,
                                            "PnL": (trade["OptionSellPrice"] - exit_avg) if exit_avg > 0 else 0,
                                            "qty": exit_qty,
                                            "ExitReason": "TARGET_HIT",
                                            "hedge_option_sell_price": exit_h_avg,
                                            "hedge_exit_time": current_time,
                                            "hedge_pnl": (exit_h_avg - trade["hedge_option_buy_price"]) if exit_h_avg > 0 else 0,
                                            "total_pnl": (trade["OptionSellPrice"] - exit_avg) + (exit_h_avg - trade["hedge_option_buy_price"])
                                        })
                                    else:
                                        reason = f"{user['user']} | {key} | Error in  exit_qty: {exit_qty},trade.get('option_symbol') exit_avg: {exit_avg},{trade.get('hedge_option_symbol')} exit_h_avg: {exit_h_avg}"
                                        logging.error(reason)
                                        send_telegram_message_admin(reason)
                                        update_trade_config_on_failure(config['KEY'], reason, user)
                                        break
                                    
                                    
                                    # DATABASE ACTIONS
                                    trade = get_clean_trade(trade)
                                    record_trade(trade, config, user['id'])
                                    delete_open_position(trade["OptionSymbol"], config, trade, user['id'])
                                    send_telegram_message(f"📤 {key} | {user['user']} {SERVER}  |  Rollover Exit {trade['Signal']}", user['telegram_chat_id'], user['telegram_token'])
                                    
                                    # If we need a NEW hedge (Expiry change or forced refresh)
                                    if expiry_match == "DIFF" or config['HEDGE_ROLLOVER_TYPE'] == 'FULL':
                                        signal = trade["Signal"]
                                        if config['HEDGE_TYPE'] == "H-P10":
                                            # hedge_result = get_optimal_option(signal, close, HEDGE_NEAREST_LTP, instruments_df, config, user)
                                            hedge_result = (None, None, None, None)
                                            for attempt in range(3):
                                                hedge_result = get_optimal_option(signal, close, HEDGE_NEAREST_LTP, instruments_df, config, user)
                                                
                                                # If the function returned a valid symbol (not None), we are done!
                                                if hedge_result[0] is not None:
                                                    break
                                                    
                                                logging.info(f"⚠️ Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                                                time.sleep(2)
                                                
                                        elif config['HEDGE_TYPE'] in ["H-M100", "H-M200"]:
                                            # hedge_result = get_hedge_option(signal, close, strike, instruments_df, config, user)
                                            hedge_result = (None, None, None, None)
                                            if strike != None :
                                                for attempt in range(3):
                                                    # Try to find the option
                                                    hedge_result = get_hedge_option(signal, close, strike, main_ltp, instruments_df, config, user)
                                                    
                                                    # If the function returned a valid symbol (not None), we are done!
                                                    if hedge_result[0] is not None:
                                                        break
                                                        
                                                    print(f"⚠️ {config['KEY']} | {user['user']} | Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                                                    logging.info(f"⚠️ {config['KEY']} | {user['user']} | Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                                                    time.sleep(2)
                                        
                                        if hedge_result:
                                            hedge_opt_symbol, hedge_strike, hedge_expiry, hedge_ltp = hedge_result
                                    
                                    # 4. PREPARE FOR ENTRY
                                    temp_trade_symbols = {
                                        "OptionSymbol": opt_symbol,
                                        "hedge_option_symbol": hedge_opt_symbol
                                    }
                                    
                                    # In SEMI + SAME + Same Qty, we skip hedge entry because Exit didn't close it
                                    skip_h_entry = False
                                    if config['HEDGE_TYPE'] == "NH":
                                        skip_h_entry = True
                                    elif config['HEDGE_ROLLOVER_TYPE'] == 'SEMI' and expiry_match == "SAME" and not qty_changed:
                                        skip_h_entry = True

                                    # 5. EXECUTE ROBUST ENTRY (Opens 65)
                                    new_qty, new_avg, new_h_avg = execute_robust_entry(temp_trade_symbols, config, user, skip_hedge_override=skip_h_entry)

                                    logging.info(f"📤{key} | Entered in {opt_symbol} with Avg price: ₹{new_avg:.2f} | Qty: {new_qty}. And  {hedge_opt_symbol} with Avg price: ₹{new_h_avg:.2f} | Qty: {new_qty}")
                                    
                                    if not is_valid_trade_data(qty, new_avg, new_h_avg, hedge_required=True):
                                        err_msg = f"⚠️ {key} | FAILED Entry: Qty ({new_qty}) or Price ({new_avg}) or ({new_h_avg}) is 0. Database NOT updated."
                                        logging.error(err_msg)
                                        send_telegram_message_admin(err_msg)
                                        break

                                    # 6. FINALIZE TRADE OBJECT
                                    trade = {
                                        "Signal": signal, "SpotEntry": close, "OptionSymbol": opt_symbol,
                                        "Strike": strike, "Expiry": expiry,
                                        "OptionSellPrice": new_avg, "EntryTime": current_time,
                                        "qty": new_qty, "interval": config['INTERVAL'], "real_trade": config['REAL_TRADE'],
                                        "EntryReason": "ROLLOVER_REENTRY", "Key": key,
                                        "hedge_option_symbol": hedge_opt_symbol,
                                        "hedge_strike": hedge_strike, 
                                        "hedge_option_buy_price": trade.get('hedge_option_sell_price') if skip_h_entry and config['HEDGE_TYPE'] != "NH" else new_h_avg,
                                        "hedge_qty": new_qty if config['HEDGE_TYPE'] != "NH" else 0,
                                        "hedge_entry_time": trade.get('hedge_entry_time') if skip_h_entry else current_time
                                    }
                                    
                                    trade = get_clean_trade(trade)
                                    save_open_position(trade, config, user['id'])
                                    logging.info(f"✅ {key} | Rollover/Reentry Complete at Qty: {new_qty}")
                                    logging.info(f"🔁 {key} | {user['user']} {SERVER}  |  Rollover Reentry {signal}\n{opt_symbol} | Avg ₹{new_avg:.2f} | Qty: {new_qty} . Hedge Symbol {hedge_opt_symbol} | @ ₹{trade['hedge_option_buy_price']:.2f}")
                                    send_telegram_message(f"🔁 {key} | {user['user']} {SERVER}  |  Rollover Reentry {signal}\n{opt_symbol} | Avg ₹{new_avg:.2f} | Qty: {new_qty} . Hedge Symbol {hedge_opt_symbol} | @ ₹{trade['hedge_option_buy_price']:.2f}", user['telegram_chat_id'], user['telegram_token'])  
                            # Rollover position :: End
                    
                    
                    random_number = random.randint(7, 15)
                    time.sleep(random_number)

            # NH STRATEGY (No Hedge) - Only execute main leg, skip all hedge logic
            elif config['HEDGE_TYPE'] == "NH":
                
                # ✅ BUY SIGNAL 
                if latest['buySignal'] and position != "BUY":
                    
                    # Exit without Hedge position and Enter new position on BUY Signal Generation.
                    if position == "SELL":
                        # 1. SETUP EXIT PARAMETERS
                        # Since this was a Main-only block, we ensure skip_hedge is True 
                        # unless the config says otherwise.
                        # EXIT CODE EXECUTION :: START
                        existing_qty = int(trade.get("qty", config['QTY']))
                        
                        print(f"📥{key} | {user['user']} {SERVER} |  Exit Signal Generated: Buying back {trade['OptionSymbol']}")
                        logging.info(f"📥{key} | {user['user']} {SERVER}  | Exit Signal Generated: Buying back {trade['OptionSymbol']}")

                        # 2. ROBUST EXIT EXECUTION
                        # This replaces place_option_hybrid_order and the avg_price None checks.
                        # It handles NH/SEMI/FULL logic. Kills thread on mismatch/failure.
                        exit_qty, avg_price, hedge_avg_price = execute_robust_exit(
                            trade, 
                            config, 
                            user, 
                            expiry_match="DIFF" # Standard signal flip forces full exit
                        )
                        logging.info(f"📤{key} | Exited without Hedge position {trade['OptionSymbol']} with Avg price: ₹{avg_price:.2f} | Qty: {exit_qty}" )

                        if not is_valid_trade_data(exit_qty, avg_price, hedge_avg_price, hedge_required=False):
                            err_msg = f"⚠️ {key} | FAILED EXIT: Qty ({exit_qty}) or Price ({avg_price}) is 0. Database NOT updated."
                            logging.error(err_msg)
                            send_telegram_message_admin(err_msg)
                            update_trade_config_on_failure(config['KEY'], err_msg, user)
                            return                         

                        # 3. UPDATE DATA & RECORD
                        trade.update({
                            "OptionBuyPrice": avg_price,
                            "SpotExit": close,
                            "ExitTime": current_time,
                            "PnL": trade["OptionSellPrice"] - avg_price,
                            "qty": exit_qty,
                            "ExitReason": "SIGNAL_GENERATED",
                            "hedge_option_sell_price": hedge_avg_price,
                            "hedge_exit_time": current_time,
                            "hedge_pnl": (hedge_avg_price - trade.get("hedge_option_buy_price", 0)) if hedge_avg_price > 0 else 0.0,
                        })
                        
                        trade["total_pnl"] = trade["PnL"] + trade.get("hedge_pnl", 0)

                        trade = get_clean_trade(trade)
                        record_trade(trade, config, user['id'])
                        delete_open_position(trade["OptionSymbol"], config, trade, user['id'])
                        
                        send_telegram_message(f"📤 {key} | {user['user']} {SERVER} | Exit Signal Generated: Buy back {trade['OptionSymbol']}\n{trade['OptionSymbol']} @ ₹{avg_price:.2f}. Profit/Qty: {trade['total_pnl']:.2f}", user['telegram_chat_id'], user['telegram_token'])
                        # EXIT CODE EXECUTION :: END
                    # --- NEW TRADE CHECKS ---
                    if config['NEW_TRADE'].lower() == "no":
                        print(f"🚫 {user['user']} | {key} | No new trades allowed. Skipping BUY signal.")
                        break

                    if check_monthly_stoploss_hit(user, config):
                        break

                    # ENTRY CODE EXECUTION :: START
                    # result = get_optimal_option("BUY", close, config['NEAREST_LTP'], instruments_df, config, user)
                    result = (None, None, None, None)
                    for attempt in range(3):
                        result = get_optimal_option("BUY", close, config['NEAREST_LTP'], instruments_df, config, user)
                        
                        # If the function returned a valid symbol (not None), we are done!
                        if result[0] is not None:
                            break
                            
                        logging.info(f"⚠️{key} | {user['user']} {SERVER} | Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                        time.sleep(2)
                        

                    if result is None or result[0] is None:
                        logging.error(f"❌{key} | {user['user']} {SERVER} | No suitable option found for BUY signal.")
                        continue
                    else:
                        opt_symbol, strike, expiry, ltp = result

                        # Prepare symbols for robust entry
                        temp_trade_symbols = {
                            "OptionSymbol": opt_symbol,
                            "hedge_option_symbol": config.get('HEDGE_SYMBOL', '-')
                        }

                        print(f"📤 {key} | {user['user']} {SERVER} | Enter Signal Generated: Selling {opt_symbol} | LTP: ₹{ltp:.2f}")
                        logging.info(f"📤 {key} | {user['user']} {SERVER} | Enter Signal Generated: Selling {opt_symbol} | LTP: ₹{ltp:.2f}")

                        # 4. ROBUST ENTRY EXECUTION
                        # This handles the Sell (Main) and optional Buy (Hedge)
                        # Replaces manual hybrid order and None checks.
                        new_qty, avg_price, hedge_avg_price = execute_robust_entry(temp_trade_symbols, config, user)
                        
                        logging.info(f"📤{key} | Entered without Hedge position {opt_symbol} with Avg price: ₹{avg_price:.2f} | Qty: {new_qty}.")
                        
                        if not is_valid_trade_data(new_qty, avg_price, hedge_avg_price, hedge_required=False):
                            err_msg = f"⚠️ {key} | FAILED ENTRY: Qty ({new_qty}) or Price ({avg_price}) is 0. Database NOT updated."
                            logging.error(err_msg)
                            send_telegram_message_admin(err_msg)
                            break                         

                        # 5. SAVE & NOTIFY
                        trade = {
                            "Signal": "BUY", "SpotEntry": close, "OptionSymbol": opt_symbol,
                            "Strike": strike, "Expiry": expiry,
                            "OptionSellPrice": avg_price, "EntryTime": current_time,
                            "qty": new_qty, "interval": config['INTERVAL'], "real_trade": config['REAL_TRADE'],
                            "EntryReason": "SIGNAL_GENERATED", "ExpiryType": config['EXPIRY'],
                            "Strategy": config['STRATEGY'], "Key": key, 
                            "hedge_option_symbol": temp_trade_symbols["hedge_option_symbol"],
                            "hedge_strike": "-", "hedge_option_buy_price": hedge_avg_price,
                            "hedge_qty": new_qty if hedge_avg_price > 0 else "-", 
                            "hedge_entry_time": current_time if hedge_avg_price > 0 else "-"
                        }
                        
                        trade = get_clean_trade(trade)
                        save_open_position(trade, config, user['id'])
                        position = "BUY"
                        
                        send_telegram_message(f"🟢{key} | BUY Entry Signal Generated\n Selling {opt_symbol} | Avg ₹{avg_price:.2f} | Qty: {new_qty}", user['telegram_chat_id'], user['telegram_token'])
                # ENTRY CODE EXECUTION :: END

                # Exit the without Hedge position and Enter new position on Sell Signal Generation.
                elif latest['sellSignal'] and position != "SELL":
                    
                    if position == "BUY":

                        existing_qty = int(trade.get("qty", config['QTY']))

                        print(f"📥 {key} | {user['user']} {SERVER} | Exit Signal Generated: Buying back {trade['OptionSymbol']} | Qty: {existing_qty}")
                        logging.info(f"📥 {key} | {user['user']} {SERVER} | Exit Signal Generated: Buying back {trade['OptionSymbol']} | Qty: {existing_qty}")

                        exit_qty, avg_price, hedge_avg_price = execute_robust_exit(
                            trade,
                            config,
                            user,
                            expiry_match="DIFF"
                        )

                        logging.info(f"📤{key} | Exited without Hedge position {trade['OptionSymbol']} with Avg price: ₹{avg_price:.2f} | Qty: {exit_qty}" )

                        if not is_valid_trade_data(exit_qty, avg_price, hedge_avg_price, hedge_required=False):
                            err_msg = f"⚠️ {key} | FAILED EXIT: Qty ({exit_qty}) or Price ({avg_price}) is 0. Database NOT updated."
                            logging.error(err_msg)
                            send_telegram_message_admin(err_msg)
                            update_trade_config_on_failure(config['KEY'], err_msg, user)
                            return

                        trade.update({
                            "OptionBuyPrice": avg_price,
                            "SpotExit": close,
                            "ExitTime": current_time,
                            "PnL": trade["OptionSellPrice"] - avg_price,
                            "qty": exit_qty,
                            "ExitReason": "SIGNAL_GENERATED",
                            "hedge_option_sell_price": hedge_avg_price,
                            "hedge_exit_time": current_time,
                            "hedge_pnl": (hedge_avg_price - trade.get("hedge_option_buy_price", 0)) if hedge_avg_price else 0.0
                        })

                        trade["total_pnl"] = trade["PnL"] + trade.get("hedge_pnl", 0)

                        trade = get_clean_trade(trade)
                        record_trade(trade, config, user['id'])
                        delete_open_position(trade["OptionSymbol"], config, trade, user['id'])

                        send_telegram_message(
                            f"📤 {user['user']} {SERVER} | {key} | {config['INTERVAL']} Exit Signal Generated:\n"
                            f" Buying {trade['OptionSymbol']} @ ₹{avg_price:.2f} | Profit/Qty: {trade['total_pnl']:.2f}",
                            user['telegram_chat_id'],
                            user['telegram_token']
                        )

                    # --------------------------------
                    # NEW TRADE RESTRICTIONS
                    # --------------------------------
                    if config['NEW_TRADE'].lower() == "no":
                        print(f"🚫 {key} | {user['user']} | No new trades allowed. Skipping SELL signal.")
                        logging.info(f"🚫 {key} | {user['user']} | No new trades allowed. Skipping SELL signal.")
                        break

                    if check_monthly_stoploss_hit(user, config):
                        break

                    # --------------------------------
                    # ENTRY PHASE
                    # --------------------------------
                    result = (None, None, None, None)
                    for attempt in range(3):
                        result = get_optimal_option("SELL", close, config['NEAREST_LTP'], instruments_df, config, user)
                        
                        # If the function returned a valid symbol (not None), we are done!
                        if result[0] is not None:
                            break
                            
                        logging.info(f"⚠️ {key} | Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                        time.sleep(2)
                        

                    if result is None or result[0] is None:
                        logging.error(f"❌ {key} | No suitable option found for SELL signal.")
                        send_telegram_message(
                            f"❌ {key} | {user['user']} {SERVER} | No suitable option found for SELL signal.",
                            user['telegram_chat_id'],
                            user['telegram_token']
                        )
                        continue

                    opt_symbol, strike, expiry, ltp = result

                    print(f"📤 {key} | {user['user']} | SELL Enter Signal Generated : Selling {opt_symbol} | Strike: {strike} | Expiry: {expiry} | LTP ₹{ltp:.2f}")
                    logging.info(f"📤 {key} | {user['user']} | SELL Enter Signal Generated : Selling {opt_symbol} | Strike: {strike} | Expiry: {expiry} | LTP ₹{ltp:.2f}")

                    temp_trade_symbols = {
                        "OptionSymbol": opt_symbol,
                        "hedge_option_symbol": config.get("HEDGE_SYMBOL", "-")
                    }

                    new_qty, avg_price, hedge_avg_price = execute_robust_entry(
                        temp_trade_symbols,
                        config,
                        user
                    )

                    logging.info(f"📤{key} | Entered without Hedge position {opt_symbol} with Avg price: ₹{avg_price:.2f} | Qty: {new_qty}.")
                    
                    if not is_valid_trade_data(new_qty, avg_price, hedge_avg_price, hedge_required=False):
                        err_msg = f"⚠️ {key} | FAILED ENTRY: Qty ({new_qty}) or Price ({avg_price}) is 0. Database NOT updated."
                        logging.error(err_msg)
                        send_telegram_message_admin(err_msg)
                        break

                    # --------------------------------
                    # SAVE TRADE
                    # --------------------------------
                    trade = {
                        "Signal": "SELL",
                        "SpotEntry": close,
                        "OptionSymbol": opt_symbol,
                        "Strike": strike,
                        "Expiry": expiry,
                        "OptionSellPrice": avg_price,
                        "EntryTime": current_time,
                        "qty": new_qty,
                        "interval": config['INTERVAL'],
                        "real_trade": config['REAL_TRADE'],
                        "EntryReason": "SIGNAL_GENERATED",
                        "ExpiryType": config['EXPIRY'],
                        "Strategy": config['STRATEGY'],
                        "Key": key,
                        "hedge_option_symbol": temp_trade_symbols["hedge_option_symbol"],
                        "hedge_strike": "-",
                        "hedge_option_buy_price": hedge_avg_price,
                        "hedge_qty": new_qty if hedge_avg_price > 0 else "-",
                        "hedge_entry_time": current_time if hedge_avg_price > 0 else "-"
                    }
                    
                    trade = get_clean_trade(trade)
                    save_open_position(trade, config, user['id'])

                    position = "SELL"

                    send_telegram_message(
                        f"🔴{key} | SELL Enter Signal Generated\n"
                        f" Sell {opt_symbol} | Avg ₹{avg_price:.2f} | Qty: {new_qty}",
                        user['telegram_chat_id'],
                        user['telegram_token']
                    )
                    logging.info(f"🔴{key} | SELL Enter Signal Generated |  Sell {opt_symbol} | Avg ₹{avg_price:.2f} | Qty: {new_qty}")

                next_candle_time = get_next_candle_time(config['INTERVAL'])
                # ✅ Add this flag before the while loop
                target_hit = False
                while datetime.datetime.now() < next_candle_time:
                    # --------------------------------
                    # POSITION MONITORING
                    # --------------------------------
                    if trade and "OptionSymbol" in trade:

                        current_ltp = get_quotes_with_retry(trade["OptionSymbol"], user)
                        entry_ltp = trade["OptionSellPrice"]

                        if current_ltp is not None and entry_ltp is not None:

                            yestime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            percent_change = round(((current_ltp - entry_ltp) / entry_ltp) * 100, 2)

                            print(
                                f"{user['user']} | {config['STRATEGY']} | {config['INTERVAL']} position at {yestime}: "
                                f"{trade['Signal']} | {trade['OptionSymbol']} | Entry LTP: ₹{entry_ltp:.2f} "
                                f"| Current LTP: ₹{current_ltp:.2f} | Chg % {percent_change} | Qty: {trade['qty']}"
                            )
                            

                    # --------------------------------
                    # INTRADAY EXIT
                    # --------------------------------
                    now = datetime.datetime.now()

                    if now.time().hour == 15 and now.time().minute >= 15 and trade and position:

                        if config['INTRADAY'] == "yes":

                            trade, position = close_position_and_no_new_trade(
                                trade, position, close, ts, config, user, key
                            )

                            msg = f"⏰ {key} | {user['user']} {SERVER} |  Intraday exit triggered"

                            print(msg)
                            logging.info(msg)

                            send_telegram_message(
                                msg,
                                user['telegram_chat_id'],
                                user['telegram_token']
                            )

                            break


                    # --------------------------------
                    # TARGET / STOPLOSS MANAGEMENT
                    # --------------------------------
                    if trade and "OptionSymbol" in trade and "OptionSellPrice" in trade and target_hit == False:

                        current_ltp = get_quotes_with_retry(trade["OptionSymbol"], user)
                        entry_ltp = trade["OptionSellPrice"]

                        # STOPLOSS
                        if check_trade_stoploss_hit(user, trade, config):

                            trade, position = close_position_and_no_new_trade(
                                trade, position, close, ts, config, user, key
                            )

                            break

                        # Target completed of 40%. Hence exiting current positions for Rollover.
                        
                        if current_ltp and entry_ltp and entry_ltp != 0 and current_ltp <= ROLLOVER_CALC * entry_ltp:

                            target_hit = True

                            print(f"📥 {key} | {SERVER} | Current {trade['OptionSymbol']} of Qty {trade['qty']} hit the target of 40%. Hence exiting position for Rollover."
                            )
                            logging.info(f"📥 {key} | {SERVER} | Current {trade['OptionSymbol']} of Qty {trade['qty']} hit the target of 40%. Hence exiting position for Rollover.")

                            exit_qty, avg_price, hedge_avg = execute_robust_exit(
                                trade,
                                config,
                                user,
                                expiry_match="DIFF"
                            )
                            logging.info(f"📤{key} | Exited without Hedge position {trade['OptionSymbol']} with Avg price: ₹{avg_price:.2f} | Qty: {exit_qty}" )

                            if not is_valid_trade_data(exit_qty, avg_price, hedge_avg, hedge_required=False):
                                err_msg = f"⚠️ {key} | FAILED EXIT: Qty ({exit_qty}) or Price ({avg_price}) is 0. Database NOT updated."
                                logging.error(err_msg)
                                send_telegram_message_admin(err_msg)
                                update_trade_config_on_failure(config['KEY'], err_msg, user)
                                return

                            trade.update({
                                "SpotExit": close,
                                "ExitTime": current_time,
                                "OptionBuyPrice": avg_price,
                                "PnL": entry_ltp - avg_price,
                                "qty": exit_qty,
                                "ExitReason": "TARGET_HIT",
                                "hedge_option_sell_price": hedge_avg,
                                "hedge_exit_time": current_time,
                                "hedge_pnl": hedge_avg - trade.get("hedge_option_buy_price",0),
                                "total_pnl": (entry_ltp - avg_price) +
                                            (hedge_avg - trade.get("hedge_option_buy_price",0))
                            })

                            trade = get_clean_trade(trade)
                            record_trade(trade, config, user['id'])
                            delete_open_position(trade["OptionSymbol"], config, trade, user['id'])

                            send_telegram_message(
                                f"📤 Exit {trade['Signal']}\n"
                                f"{trade['OptionSymbol']} @ ₹{avg_price:.2f} | "
                                f"PnL ₹{trade['total_pnl']:.2f}",
                                user['telegram_chat_id'],
                                user['telegram_token']
                            )
                            logging.info(f"🔴 {key} | {user['user']} {SERVER} | Target triggered for {trade['OptionSymbol']} at ₹{current_ltp:.2f}")


                            last_expiry = trade["Expiry"]
                            signal = trade["Signal"]

                            trade = {}

                            # --------------------------------
                            # REENTRY RULES
                            # --------------------------------

                            if config['NEW_TRADE'].lower() == "no":
                                position = None
                                break

                            if check_monthly_stoploss_hit(user, config):
                                break

                            result = (None, None, None, None)
                            
                            for attempt in range(3):
                                result = get_optimal_option(signal, close, config['NEAREST_LTP'], instruments_df, config, user)
                                
                                # If the function returned a valid symbol (not None), we are done!
                                if result[0] is not None:
                                    break
                                    
                                logging.info(f"⚠️ {key} | Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                                time.sleep(2)
                                

                            if result is None or result[0] is None:

                                logging.error(
                                    f"❌ {key} | No expiry found after {last_expiry}"
                                )

                                position = None
                                continue

                            opt_symbol, strike, expiry, ltp = result

                            print(
                                f"🔁 {key} | Rollover Signal Generated {signal}: {opt_symbol} | Strike {strike} | Expiry {expiry}"
                            )
                            logging.info(f"🔁 {key} | Rollover Signal Generated {signal}: {opt_symbol} | Strike {strike} | Expiry {expiry}")

                            symbols = {
                                "OptionSymbol": opt_symbol,
                                "hedge_option_symbol": config.get("HEDGE_SYMBOL","-")
                            }

                            new_qty, avg_price, hedge_avg = execute_robust_entry(symbols, config, user)

                            logging.info(f"📤{key} | Entered without Hedge position {opt_symbol} with Avg price: ₹{avg_price:.2f} | Qty: {new_qty}.")
                            
                            if not is_valid_trade_data(new_qty, avg_price, hedge_avg, hedge_required=False):
                                err_msg = f"⚠️ {key} | FAILED ENTRY: Qty ({new_qty}) or Price ({avg_price}) is 0. Database NOT updated."
                                logging.error(err_msg)
                                send_telegram_message_admin(err_msg)
                                break 
                            
                            trade = {
                                "Signal": signal,
                                "SpotEntry": close,
                                "OptionSymbol": opt_symbol,
                                "Strike": strike,
                                "Expiry": expiry,
                                "OptionSellPrice": avg_price,
                                "EntryTime": current_time,
                                "qty": new_qty,
                                "interval": config['INTERVAL'],
                                "real_trade": config['REAL_TRADE'],
                                "EntryReason": "ROLLOVER",
                                "ExpiryType": config['EXPIRY'],
                                "Strategy": config['STRATEGY'],
                                "Key": key,
                                "hedge_option_symbol": symbols["hedge_option_symbol"],
                                "hedge_strike": "-",
                                "hedge_option_buy_price": hedge_avg,
                                "hedge_qty": new_qty if hedge_avg > 0 else "-",
                                "hedge_entry_time": current_time if hedge_avg > 0 else "-"
                            }

                            trade = get_clean_trade(trade)
                            save_open_position(trade, config, user['id'])

                            send_telegram_message(
                                f"🔁 {key} | Reentry {signal}\n"
                                f"{opt_symbol} | Avg ₹{avg_price:.2f} | Qty {new_qty}",
                                user['telegram_chat_id'],
                                user['telegram_token']
                            )
                            logging.info(f"🔁 {key} | Reentry {signal} {opt_symbol} | Avg ₹{avg_price:.2f} | Qty {new_qty}")

                            position = signal                    
                    
                    random_number = random.randint(7, 15)
                    time.sleep(random_number)

                
        except ReadTimeout as re:
            # Ignore read timeout
            logging.error(f"⚠️ {user['user']} {SERVER}  |  {key}  | Exception: {re}", exc_info=True)
            pass


        except exceptions.NetworkException:
            # Ignore network exception
            pass

        
        except Exception as e:
            logging.error(f"{user['user']} {SERVER}  | Exception: {e}", exc_info=True)
            send_telegram_message(f"⚠️ {user['user']} {SERVER}  |  {key}  |  {config['INTERVAL']} Error: {e}",user['telegram_chat_id'], user['telegram_token'])
            time.sleep(60)



# ====== Run ======
def init_and_run(user):
    while True:
        try:
            who_tried(user)
            
            instruments_df = pd.read_csv(INSTRUMENTS_FILE)
            threads = []


            configs = get_trade_configs(user['id'])
            keys = configs.keys()
            
            for key in keys:
                config = configs[key]
                init_db()
                t = threading.Thread(target=live_trading, args=(instruments_df, config, key, user))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            break
        except Exception as e:
            logging.error(f"Fatal error: {e}")
            logging.error("Restarting emalive in 10 seconds...")
            time.sleep(10)


# def execute_robust_exit_2026_03_17(trade, config, user, expiry_match="DIFF"):
#     """
#     STRICT EXIT LOGIC:
#     - NH / FULL / DIFF Expiry: Exit Both.
#     - SEMI + SAME Expiry: 
#         - If Qty Changed (130 -> 65): Exit BOTH to reset.
#         - If Qty Same: Exit Main Only.
#     - Failure: Any mismatch or partial results in KILLED THREAD.
#     """
#     target_qty_new = int(config.get('QTY', 0))
#     existing_qty = int(trade.get('qty', 0))
#     hr_type = str(config.get('HEDGE_ROLLOVER_TYPE', 'FULL')).upper()
#     h_type = str(config.get('HEDGE_TYPE', 'FULL')).upper()
#     qty_changed = (target_qty_new != existing_qty)
    

#     # --- EXIT GATEKEEPER ---
#     skip_hedge = False 
#     if h_type == "NH":
#         skip_hedge = True
#     elif hr_type == "SEMI" and expiry_match == "SAME":
#         # If qty changed, we must clear the old hedge too
#         skip_hedge = False if qty_changed else True

#     main_filled_total = 0
#     main_total_val = 0.0
#     hedge_filled_total = 0
#     hedge_total_val = 0.0

#     for attempt in range(1, 4):
#         remaining = existing_qty - main_filled_total
#         if remaining <= 0: break

#         logging.info(f"🔄 {user['user']} | EXIT Attempt {attempt}/3 | Target: {existing_qty}")

#         # Main Leg Leads
#         m_id, m_avg, m_f = place_robust_limit_order(trade["OptionSymbol"], remaining, "BUY", config, user, action="EXIT")
#         if m_f > 0:
#             main_total_val += (m_avg * m_f)
#             main_filled_total += m_f
#             if not skip_hedge:
#                 h_id, h_avg, h_f = place_robust_limit_order(trade["hedge_option_symbol"], m_f, "SELL", config, user, action="EXIT")
#                 if h_f > 0:
#                     hedge_total_val += (h_avg * h_f)
#                     hedge_filled_total += h_f
#         time.sleep(2)

#     # RECONCILIATION
#     mismatch = (not skip_hedge and main_filled_total != hedge_filled_total)
#     incomplete = (main_filled_total < existing_qty)

#     if mismatch or incomplete:
#         reason = f"EXIT FAIL: M:{main_filled_total} H:{hedge_filled_total} vs Target:{existing_qty}"
#         update_trade_config_on_failure(config['KEY'], reason, user)
#         logging.critical(f"☢️ {user['user']} | THREAD KILLED: {reason}")
#         time.sleep(10)
#         sys.exit(reason)

#     final_m_avg = main_total_val / main_filled_total
#     final_h_avg = hedge_total_val / hedge_filled_total if hedge_filled_total > 0 else 0
#     return main_filled_total, final_m_avg, final_h_avg


# def execute_robust_entry_2026_03_17(trade, config, user, skip_hedge_override=None):
#     """
#     STRICT ENTRY + SELF-HEALING REVERSAL:
#     - Target: config['QTY'].
#     - Rule 1: Partial fills OK if Matched (10 = 10).
#     - Rule 2: If Mismatch (Hedge 40, Main 10), reverse extra 30.
#     - Rule 3: If still mismatched after reversal, KILL THREAD.
#     """
#     target_qty = int(config.get('QTY', 0))
#     h_type = str(config.get('HEDGE_TYPE', 'FULL')).upper()
#     skip_hedge = skip_hedge_override if skip_hedge_override is not None else (h_type == "NH")

#     main_filled_total = 0
#     main_total_val = 0.0
#     hedge_filled_total = 0
#     hedge_total_val = 0.0

#     for attempt in range(1, 4):
#         remaining = target_qty - (main_filled_total if skip_hedge else hedge_filled_total)
#         if remaining <= 0: break

#         logging.info(f"🔄 {user['user']} | ENTRY Attempt {attempt}/3 | Target: {target_qty}")

#         if skip_hedge:
#             m_id, m_avg, m_f = place_robust_limit_order(trade["OptionSymbol"], remaining, "SELL", config, user, action="ENTRY")
#             if m_f > 0:
#                 main_total_val += (m_avg * m_f); main_filled_total += m_f
#         else:
#             # 1. Lead with Hedge (BUY)
#             h_id, h_avg, h_f = place_robust_limit_order(trade["hedge_option_symbol"], remaining, "BUY", config, user, action="ENTRY")
#             if h_f > 0:
#                 # 2. Match Main (SELL)
#                 m_id, m_avg, m_f = place_robust_limit_order(trade["OptionSymbol"], h_f, "SELL", config, user, action="ENTRY")

#                 if m_f == h_f:
#                     # ✅ Matched (Even if partial) - GO AHEAD
#                     main_total_val += (m_avg * m_f); main_filled_total += m_f
#                     hedge_total_val += (h_avg * h_f); hedge_filled_total += h_f
#                 else:
#                     # ❌ Mismatch - Attempt Reversal
#                     unhedged = h_f - m_f
#                     logging.warning(f"⚠️ Mismatch: H:{h_f} M:{m_f}. Reversing {unhedged} extra Hedge...")
#                     reversed_qty = 0
#                     for rev_att in range(1, 4):
#                         _, _, r_f = place_robust_limit_order(trade["hedge_option_symbol"], (unhedged - reversed_qty), "SELL", config, user, action="EXIT")
#                         reversed_qty += r_f
#                         if reversed_qty >= unhedged: break
#                         time.sleep(1)

#                     # Final Balance Check after reversal
#                     retained_h = h_f - reversed_qty
#                     if retained_h == m_f:
#                         logging.info(f"✅ Self-Heal Success: Balanced at {m_f}. Continuing...")
#                         main_total_val += (m_avg * m_f); main_filled_total += m_f
#                         hedge_total_val += (h_avg * retained_h); hedge_filled_total += retained_h
#                     else:
#                         # Still Mismatched - KILL
#                         reason = f"ENTRY MISMATCH: H:{retained_h} != M:{m_f} after reversal."
#                         update_trade_config_on_failure(config['KEY'], reason, user)
#                         logging.critical(f"☢️ {user['user']} | {reason}")
#                         sys.exit(reason)
#         time.sleep(2)

#     final_m_avg = main_total_val / main_filled_total if main_filled_total > 0 else 0
#     final_h_avg = hedge_total_val / hedge_filled_total if hedge_filled_total > 0 else 0
#     return main_filled_total, final_m_avg, final_h_avg


def execute_robust_entry(trade, config, user, skip_hedge_override=None):
    """
    SINGLE-SHOT ENTRY:
    Executes Hedge and Main Leg with mismatch recovery logic.
    """
    target_qty = int(config.get('QTY', 0))
    h_type = str(config.get('HEDGE_TYPE', 'FULL')).upper()
    skip_hedge = skip_hedge_override if skip_hedge_override is not None else (h_type == "NH")

    main_filled_total = 0
    main_total_val = 0.0
    hedge_filled_total = 0
    hedge_total_val = 0.0

    logging.info(f"🚀 Starting {user['user']} ENTRY | Target: {target_qty}")
    
    # PRE-TRADE PRICE VALIDATION
    if not validate_trade_prices(trade["OptionSymbol"], trade["hedge_option_symbol"], config, user):
        logging.warning("🛑 ENTRY ABORTED: Price validation failed.")
        return 0, 0, 0
    
    if skip_hedge:
        m_id, m_avg, m_f = place_robust_limit_order(trade["OptionSymbol"], target_qty, "SELL", config, user, action="ENTRY")
        if m_f > 0:
            main_total_val = (m_avg * m_f)
            main_filled_total = m_f
    else:
        # Step 1: Hedge BUY
        h_id, h_avg, h_f = place_robust_limit_order(trade["hedge_option_symbol"], target_qty, "BUY", config, user, action="ENTRY")
        
        if h_f > 0:
            # Step 2: Main SELL
            m_id, m_avg, m_f = place_robust_limit_order(trade["OptionSymbol"], h_f, "SELL", config, user, action="ENTRY")

            if m_f == h_f:
                main_total_val = (m_avg * m_f); main_filled_total = m_f
                hedge_total_val = (h_avg * h_f); hedge_filled_total = h_f
            else:
                # Step 3: Mismatch Recovery
                unhedged = h_f - m_f
                logging.warning(f"⚠️ Mismatch! H:{h_f} M:{m_f}. Reversing {unhedged} Hedge...")
                _, _, r_f = place_robust_limit_order(trade["hedge_option_symbol"], unhedged, "SELL", config, user, action="EXIT")
                
                retained_h = h_f - r_f
                if retained_h == m_f:
                    logging.info(f"✅ Recovery Success: Balanced at {m_f}.")
                    main_total_val = (m_avg * m_f); main_filled_total = m_f
                    hedge_total_val = (h_avg * retained_h); hedge_filled_total = retained_h
                else:
                    reason = f"CRITICAL MISMATCH: H:{retained_h} != M:{m_f} after reversal."
                    update_trade_config_on_failure(config['KEY'], reason, user)
                    logging.critical(f"☢️ {reason}")
                    sys.exit(reason)

    final_m_avg = main_total_val / main_filled_total if main_filled_total > 0 else 0
    final_h_avg = hedge_total_val / hedge_filled_total if hedge_filled_total > 0 else 0
    
    logging.info(f"🏁 ENTRY SUMMARY | Main: {main_filled_total} @ {round(final_m_avg, 2)} | Hedge: {hedge_filled_total} @ {round(final_h_avg, 2)}")
    return main_filled_total, final_m_avg, final_h_avg


def execute_robust_exit(trade, config, user, expiry_match="DIFF"):
    """
    SINGLE-SHOT STRICT EXIT:
    - Removes 3-attempt loop; relies on 5s robust price chasing.
    - If Main exits and Hedge fails (or vice versa), triggers KILL THREAD.
    """
    target_qty_new = int(config.get('QTY', 0))
    existing_qty = int(trade.get('qty', 0))
    hr_type = str(config.get('HEDGE_ROLLOVER_TYPE', 'FULL')).upper()
    h_type = str(config.get('HEDGE_TYPE', 'FULL')).upper()
    qty_changed = (target_qty_new != existing_qty)

    # --- EXIT GATEKEEPER ---
    skip_hedge = False 
    if h_type == "NH":
        skip_hedge = True
    elif hr_type == "SEMI" and expiry_match == "SAME":
        # If qty changed, we must clear the old hedge too
        skip_hedge = False if qty_changed else True

    main_filled_total = 0
    main_total_val = 0.0
    hedge_filled_total = 0
    hedge_total_val = 0.0

    logging.info(f"🚪 {user['user']} | Starting EXIT | Target Qty: {existing_qty} | Skip Hedge: {skip_hedge}")

    # 1. Main Leg Leads (BUY to exit a SELL position)
    m_id, m_avg, m_f = place_robust_limit_order(
        trade["OptionSymbol"], existing_qty, "BUY", config, user, action="EXIT"
    )
    
    if m_f > 0:
        main_total_val = (m_avg * m_f)
        main_filled_total = m_f
        
        # 2. Exit Hedge if required (SELL to exit a BUY hedge)
        if not skip_hedge:
            # We exit only the amount of hedge that corresponds to the filled main leg
            h_id, h_avg, h_f = place_robust_limit_order(
                trade["hedge_option_symbol"], m_f, "SELL", config, user, action="EXIT"
            )
            if h_f > 0:
                hedge_total_val = (h_avg * h_f)
                hedge_filled_total = h_f

    # --- RECONCILIATION & KILL SWITCH ---
    # Case A: Hedge mismatch (Main filled 100, Hedge filled 50)
    mismatch = (not skip_hedge and main_filled_total != hedge_filled_total)
    # Case B: Incomplete exit (Target was 100, but only 80 filled)
    incomplete = (main_filled_total < existing_qty)

    if mismatch or incomplete:
        reason = f"EXIT FAILURE: M:{main_filled_total} H:{hedge_filled_total} vs Target:{existing_qty}"
        update_trade_config_on_failure(config['KEY'], reason, user)
        logging.critical(f"☢️ {user['user']} | THREAD KILLED: {reason}")
        # Give some time for logs to flush before exiting
        time.sleep(5)
        sys.exit(reason)

    final_m_avg = main_total_val / main_filled_total
    final_h_avg = hedge_total_val / hedge_filled_total if hedge_filled_total > 0 else 0
    
    logging.info(f"🏁 EXIT Complete | Main: {main_filled_total} @ {round(final_m_avg, 2)} | Hedge: {hedge_filled_total}")
    return main_filled_total, final_m_avg, final_h_avg

