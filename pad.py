# --- ENTRY CODE EXECUTION :: START ---
try:
    hedge_required = config['HEDGE_TYPE'] != "NH"
    # Set the offset based on config
    h_offset = 200 if config['HEDGE_TYPE'] == "H-M200" else 100
    
    result = (None, None, None, None, None)
    
    # 1. UNIFIED SEARCH
    for attempt in range(3):
        if config['HEDGE_TYPE'] == "H-P10":
            # For P10, find Main first (Hedge Required=False for this call)
            result = get_robust_optimal_option(signal="BUY", spot=close, nearest_price=config['NEAREST_LTP'], 
                                             instruments_df=instruments_df, config=config, user=user, hedge_required=False)
            
            # Then find P10 Hedge separately
            m_sym, m_strike, m_exp, m_ltp, _ = result
            if m_sym:
                h_res = get_robust_optimal_option(signal="BUY", spot=close, nearest_price=HEDGE_NEAREST_LTP, 
                                                instruments_df=instruments_df, config=config, user=user, hedge_required=False)
                if h_res[0]:
                    # Combine Main from first call and Hedge from second call
                    result = (m_sym, m_strike, m_exp, m_ltp, h_res[0])
                else: result = (None,) * 5
        else:
            # For M100/M200 or NH, use the single robust call (It handles the offset internally)
            result = get_robust_optimal_option(signal="BUY", spot=close, nearest_price=config['NEAREST_LTP'], 
                                             instruments_df=instruments_df, config=config, user=user, 
                                             hedge_offset=h_offset, hedge_required=hedge_required)
        
        if result[0] is not None: break
        time.sleep(2)

    # 2. VALIDATION & EXECUTION
    opt_symbol, strike, expiry, ltp, hedge_opt_symbol = result

    if opt_symbol is None or (hedge_required and hedge_opt_symbol is None):
        logging.error(f"❌{key} | No Pair Found.")
        continue 
    
    # 3. EXECUTE ORDERS
    temp_trade_symbols = {"OptionSymbol": opt_symbol, "hedge_option_symbol": hedge_opt_symbol}
    qty, avg_price, hedge_avg_price = execute_robust_entry(temp_trade_symbols, config, user)

    if is_valid_trade_data(qty, avg_price, hedge_avg_price, hedge_required):
        trade = {
            "Signal": "BUY", "SpotEntry": close, "OptionSymbol": opt_symbol, "Strike": strike, "Expiry": expiry,
            "OptionSellPrice": avg_price, "EntryTime": current_time, "qty": qty, "Key": key, 
            "hedge_option_symbol": hedge_opt_symbol, "hedge_option_buy_price": hedge_avg_price, "hedge_qty": qty if hedge_required else 0
        }
        save_open_position(get_clean_trade(trade), config, user['id'])
        send_telegram_message(f"🟢{key} | Executed: {opt_symbol} + {hedge_opt_symbol}", user['telegram_chat_id'], user['telegram_token'])

except Exception as e:
    logging.error(f"❌ Entry Error: {e}")
