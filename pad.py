# --- ENTRY CODE EXECUTION (SELL) :: START ---
try:
    h_type = config['HEDGE_TYPE']
    h_required = h_type != "NH"
    h_offset = 200 if h_type == "H-M200" else 100
    
    result = (None, None, None, None, None)
    hedge_result = (None, None, None, None, None)

    # 1. FIND MAIN OPTION (SELL)
    for attempt in range(3):
        # We use hedge_required=False to find the Main CE first
        result = get_robust_optimal_option(
            signal="SELL", 
            spot=close, 
            nearest_price=config['NEAREST_LTP'], 
            instruments_df=instruments_df, 
            config=config, 
            user=user, 
            hedge_required=False
        )
        
        # Unpacks: (opt_symbol, strike, expiry, ltp, _)
        if result[0] is not None:
            break
            
        logging.info(f"⚠️{key}  |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
        time.sleep(2)

    print(f"📤 {key}  | {user['user']} {SERVER} | Signal Generated for Entry : Optimal option search completed with result: {result}")
    logging.info(f"📤 {key} | {user['user']} {SERVER} | Signal Generated for Entry : Optimal option search completed with result: {result}")
    
    opt_symbol, strike, expiry, ltp, _ = result
    
    # 2. FIND HEDGE OPTION (SELL)
    if opt_symbol is not None and h_required:
        # CASE A: Price-based Hedge (H-P10)
        if h_type == "H-P10":
            for attempt in range(3):
                hedge_result = get_robust_optimal_option(
                    signal="SELL", 
                    spot=close, 
                    nearest_price=HEDGE_NEAREST_LTP, 
                    instruments_df=instruments_df, 
                    config=config, 
                    user=user, 
                    hedge_required=False
                )
                if hedge_result[0] is not None:
                    break
                logging.info(f"⚠️{key} |  Search Attempt {attempt+1} failed to find an option within tolerance. Retrying in 2s...")
                time.sleep(2)
                
        # CASE B: Offset-based Hedge (H-M100 / H-M200)
        elif h_type in ["H-M100", "H-M200"]:
            for attempt in range(3):
                # Search for the fixed offset CE Hedge strike (+ h_offset)
                h_search = get_robust_optimal_option(
                    signal="SELL", 
                    spot=close, 
                    nearest_price=0, # Fixed offset search
                    instruments_df=instruments_df, 
                    config=config, 
                    user=user, 
                    hedge_offset=h_offset, 
                    hedge_required=True
                )
                
                # The 5th index of robust return is the calculated hedge symbol
                if h_search[4] is not None:
                    h_sym = h_search[4]
                    h_q = get_entire_quote(h_sym, user)
                    hedge_result = (h_sym, strike + h_offset, expiry, h_q.get('last_price', 0))
                    break
                    
                print(f"⚠️{key} | {user['user']} Search Attempt {attempt+1} failed to find an Hedge option. Retrying in 2s...")
                logging.info(f"⚠️{key} | {user['user']} Search Attempt {attempt+1} failed to find an Hedge option. Retrying in 2s...")
                time.sleep(2)

    # 3. VALIDATION & ERROR HANDLING
    if result[0] is None or (h_required and (hedge_result is None or hedge_result[0] is None)):
        logging.error(f"❌{config['KEY']} | {SERVER}: No suitable option found for SELL signal.")
        send_telegram_message(f"❌{config['KEY']} | {SERVER}: No suitable option found for SELL signal.",user['telegram_chat_id'], user['telegram_token'])
        
        err_msg = f"❌{config['KEY']} | {SERVER} : "
        err_msg += "No Main option found" if result[0] is None else "No Hedge option found"
        send_telegram_message_admin(err_msg)
        # continue (Assuming this is inside a loop)
    else:
        # 4. EXECUTION
        opt_symbol, strike, expiry, ltp, _ = result
        hedge_opt_symbol, hedge_strike, hedge_expiry, hedge_ltp = hedge_result if hedge_result else (None, None, None, 0)

        temp_trade_symbols = {
            "OptionSymbol": opt_symbol,
            "hedge_option_symbol": hedge_opt_symbol
        }

        print(f"📤{key} | Entering SELL Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")
        logging.info(f"📤 {key} | Entering SELL Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")

        qty, avg_price, hedge_avg_price = execute_robust_entry(temp_trade_symbols, config, user)

        logging.info(f"📤{key} | Entered in {opt_symbol} @ ₹{avg_price:.2f} | Qty: {qty}. Hedge {hedge_opt_symbol} @ ₹{hedge_avg_price:.2f}")
        
        if not is_valid_trade_data(qty, avg_price, hedge_avg_price, hedge_required=h_required):
            err_msg = f"⚠️ {key} | FAILED Entry: Qty or Price is 0. Database NOT updated."
            logging.error(err_msg)
            send_telegram_message_admin(err_msg)
            # break (Assuming this is inside a loop)

        # 5. DB SAVE & TELEGRAM
        trade = get_clean_trade({
            "Signal": "SELL", "SpotEntry": close, "OptionSymbol": opt_symbol,
            "Strike": strike, "Expiry": expiry,
            "OptionSellPrice": avg_price, "EntryTime": current_time,
            "qty": qty,  "interval": config['INTERVAL'], "real_trade": config['REAL_TRADE'],
            "EntryReason":"SIGNAL_GENERATED", "ExpiryType":config['EXPIRY'],
            "Strategy":config['STRATEGY'], "Key":key,
            "hedge_option_symbol":hedge_opt_symbol,
            "hedge_strike":hedge_strike, "hedge_option_buy_price":hedge_avg_price,
            "hedge_qty": qty if h_required else 0, 
            "hedge_entry_time": current_time
        })
        
        save_open_position(trade, config, user['id'])
        position = "SELL"
        msg = f"🔴 {key} | {user['user']} {SERVER} | Sell Signal {opt_symbol} | Avg ₹{avg_price:.2f} | Qty: {qty}. Hedge {hedge_opt_symbol} @ ₹{hedge_avg_price:.2f}"
        logging.info(msg)
        send_telegram_message(msg, user['telegram_chat_id'], user['telegram_token'])

except Exception as e:
    logging.error(f"❌ SELL Entry Execution Critical Error: {str(e)}")
# --- ENTRY CODE EXECUTION (SELL) :: END ---
