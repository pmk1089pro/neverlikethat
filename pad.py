# --- ENTRY CODE EXECUTION :: START ---
try:
    # 1. Determine if hedge is required based on config
    hedge_required = config['HEDGE_TYPE'] != "NH"
    
    # 2. Determine hedge offset (M200, M100, or Default)
    h_offset = 200 if config['HEDGE_TYPE'] == "H-M200" else 100
    
    # 3. Unified Search (Main + Hedge in one go)
    result = (None, None, None, None, None)
    for attempt in range(3):
        # Using the new robust function
        result = get_robust_optimal_option(
            signal="BUY", 
            spot=close, 
            nearest_price=config['NEAREST_LTP'], 
            instruments_df=instruments_df, 
            config=config, 
            user=user, 
            hedge_offset=h_offset, 
            hedge_required=hedge_required
        )
        
        # result returns: (opt_symbol, strike, expiry, ltp, hedge_opt_symbol)
        if result[0] is not None:
            break
            
        logging.info(f"⚠️{key} | {user['user']} | Search Attempt {attempt+1} failed to find a valid pair. Retrying in 2s...")
        time.sleep(2)

    # 4. Final Validation Check
    opt_symbol, strike, expiry, ltp, hedge_opt_symbol = result

    if opt_symbol is None or (hedge_required and hedge_opt_symbol is None):
        logging.error(f"❌{key} | INTERVAL {config['INTERVAL']} | {user['user']} {SERVER} | No suitable option found for BUY signal.")
        send_telegram_message(f"❌{key} | INTERVAL {config['INTERVAL']} | {user['user']} {SERVER} | No suitable option found for BUY signal.", user['telegram_chat_id'], user['telegram_token'])
        
        err_msg = f"❌{key} | {user['user']} {SERVER} | "
        err_msg += "Main option missing" if opt_symbol is None else "Hedge option missing/illiquid"
        send_telegram_message_admin(err_msg)
        # Skip this user/cycle
        # continue (Note: Ensure this is inside your user loop)
    else:
        # 5. Prepare and Execute Entry
        temp_trade_symbols = {
            "OptionSymbol": opt_symbol,
            "hedge_option_symbol": hedge_opt_symbol
        }

        print(f"📤{key} | {user['user']} {SERVER} | Entering Entry Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")
        logging.info(f"📤 {key} | Entering Entry Sequence for {opt_symbol} with Hedge {hedge_opt_symbol}")

        # Execute order (Ensure hedge is fired first inside this function)
        qty, avg_price, hedge_avg_price = execute_robust_entry(temp_trade_symbols, config, user)

        logging.info(f"📤{key} | Entered in {opt_symbol} @ ₹{avg_price:.2f} | Qty: {qty}. Hedge {hedge_opt_symbol} @ ₹{hedge_avg_price:.2f}")
        
        # Validate trade execution results
        if not is_valid_trade_data(qty, avg_price, hedge_avg_price, hedge_required=hedge_required):
            err_msg = f"⚠️ {key} | FAILED Entry: Qty ({qty}) or Price ({avg_price}) is 0. Database NOT updated."
            logging.error(err_msg)
            send_telegram_message_admin(err_msg)
            # break or continue based on your outer loop logic
        else:
            # 6. Save Position and Notify
            trade = {
                "Signal": "BUY", 
                "SpotEntry": close, 
                "OptionSymbol": opt_symbol,
                "Strike": strike, 
                "Expiry": expiry,
                "OptionSellPrice": avg_price, 
                "EntryTime": current_time,
                "qty": qty, 
                "interval": config['INTERVAL'], 
                "real_trade": config['REAL_TRADE'],
                "EntryReason": "SIGNAL_GENERATED", 
                "ExpiryType": config['EXPIRY'],
                "Strategy": config['STRATEGY'], 
                "Key": key, 
                "hedge_option_symbol": hedge_opt_symbol,
                "hedge_strike": strike - h_offset if hedge_required else None,
                "hedge_option_buy_price": hedge_avg_price,
                "hedge_qty": qty if hedge_required else 0, 
                "hedge_entry_time": current_time
            }
            
            trade = get_clean_trade(trade)
            save_open_position(trade, config, user['id'])
            position = "BUY"
            
            msg = (f"🟢{key} | {user['user']} {SERVER} | Buy Signal\n"
                   f"{opt_symbol} | Avg ₹{avg_price:.2f} | Qty: {qty}\n"
                   f"Hedge: {hedge_opt_symbol} | Avg ₹{hedge_avg_price:.2f}")
            send_telegram_message(msg, user['telegram_chat_id'], user['telegram_token'])

except Exception as e:
    logging.error(f"❌ Critical Error in Entry Execution: {str(e)}")
# --- ENTRY CODE EXECUTION :: END ---
