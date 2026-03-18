# --- ENTRY CODE EXECUTION :: START ---
try:
    # 1. INITIALIZE VARIABLES
    hedge_required = config['HEDGE_TYPE'] != "NH"
    result = (None, None, None, None, None)
    hedge_result = (None, None, None, None, None)
    
    # 2. FIND MAIN OPTION (3 Attempts)
    # We call it once to get the best Main strike near your target LTP
    for attempt in range(3):
        result = get_robust_optimal_option(
            signal="BUY", 
            spot=close, 
            nearest_price=config['NEAREST_LTP'], 
            instruments_df=instruments_df, 
            config=config, 
            user=user, 
            hedge_required=False  # Find Main first
        )
        if result[0] is not None:
            break
        logging.info(f"⚠️{key} | {user['user']} | Main Search Attempt {attempt+1} failed. Retrying...")
        time.sleep(2)

    opt_symbol, strike, expiry, m_ltp, _ = result

    # 3. HEDGE LOGIC BRANCHING (H-P10 vs H-M100/200)
    if opt_symbol is not None and hedge_required:
        
        # CASE A: Price-based Hedge (Search for specific LTP, e.g., 10)
        if config['HEDGE_TYPE'] == "H-P10":
            for attempt in range(3):
                hedge_result = get_robust_optimal_option(
                    signal="BUY", 
                    spot=close, 
                    nearest_price=HEDGE_NEAREST_LTP, 
                    instruments_df=instruments_df, 
                    config=config, 
                    user=user, 
                    hedge_required=False
                )
                if hedge_result[0] is not None:
                    break
                time.sleep(2)

        # CASE B: Offset-based Hedge (M100 or M200 points away from Main Strike)
        elif config['HEDGE_TYPE'] in ["H-M100", "H-M200"]:
            h_offset = 200 if config['HEDGE_TYPE'] == "H-M200" else 100
            for attempt in range(3):
                # Using the specialized hedge finder for fixed offsets
                hedge_result = get_hedge_option(
                    signal="BUY", 
                    spot=close, 
                    main_strike=strike, 
                    main_ltp=m_ltp, 
                    instruments_df=instruments_df, 
                    config=config, 
                    user=user, 
                    offset=h_offset
                )
                if hedge_result[0] is not None:
                    break
                time.sleep(2)

    # 4. FINAL VALIDATION BEFORE ORDER
    # Check if Main exists, and if Hedge is required, check if Hedge exists
    if opt_symbol is None or (hedge_required and hedge_result[0] is None):
        logging.error(f"❌{key} | {user['user']} | No suitable option found for BUY signal.")
        err_msg = f"❌{key} | {user['user']} | Main: {opt_symbol} | Hedge: {hedge_result[0]}"
        send_telegram_message(err_msg, user['telegram_chat_id'], user['telegram_token'])
        send_telegram_message_admin(err_msg)
        # return/continue depending on your loop
    else:
        # 5. EXECUTE ENTRY
        hedge_opt_symbol, hedge_strike, hedge_expiry, hedge_ltp, _ = hedge_result if hedge_result else (None, None, None, 0, None)
        
        temp_trade_symbols = {
            "OptionSymbol": opt_symbol,
            "hedge_option_symbol": hedge_opt_symbol
        }

        print(f"📤{key} | {user['user']} | Entering {opt_symbol} with Hedge {hedge_opt_symbol}")
        
        # This function must buy the Hedge first for margin benefits!
        qty, avg_price, hedge_avg_price = execute_robust_entry(temp_trade_symbols, config, user)

        # 6. LOGGING & DB SAVE
        if not is_valid_trade_data(qty, avg_price, hedge_avg_price, hedge_required=hedge_required):
            err_msg = f"⚠️ {key} | FAILED Entry: Qty or Price is 0. Database NOT updated."
            logging.error(err_msg)
            send_telegram_message_admin(err_msg)
        else:
            trade = {
                "Signal": "BUY", "SpotEntry": close, "OptionSymbol": opt_symbol,
                "Strike": strike, "Expiry": expiry,
                "OptionSellPrice": avg_price, "EntryTime": current_time,
                "qty": qty, "interval": config['INTERVAL'], "real_trade": config['REAL_TRADE'],
                "EntryReason": "SIGNAL_GENERATED", "ExpiryType": config['EXPIRY'],
                "Strategy": config['STRATEGY'], "Key": key, "hedge_option_symbol": hedge_opt_symbol,
                "hedge_strike": hedge_strike, "hedge_option_buy_price": hedge_avg_price,
                "hedge_qty": qty if hedge_required else 0, 
                "hedge_entry_time": current_time
            }
            
            save_open_position(get_clean_trade(trade), config, user['id'])
            
            msg = (f"🟢{key} | Buy Signal\nMain: {opt_symbol} @ ₹{avg_price:.2f}\n"
                   f"Hedge: {hedge_opt_symbol} @ ₹{hedge_avg_price:.2f}\nQty: {qty}")
            send_telegram_message(msg, user['telegram_chat_id'], user['telegram_token'])

except Exception as e:
    logging.error(f"❌ CRITICAL ERROR IN ENTRY: {e}")
# --- ENTRY CODE EXECUTION :: END ---
