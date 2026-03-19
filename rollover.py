# --- REENTRY / ROLLOVER LOGIC (EXPERT CONSOLIDATED - FINAL) ---
result = None
signal = trade["Signal"]

# Map hedge parameters for your robust function
h_offset = 200 if config.get('HEDGE_TYPE') == "H-M200" else 100
h_req = True if config.get('HEDGE_TYPE') in ["H-M100", "H-M200"] else False

# 1. PRIMARY SEARCH: REPLACES get_optimal_option
for attempt in range(3):
    # Returns 5-tuple: (opt_symbol, opt_strike, opt_expiry, opt_ltp, h_sym)
    search_res = get_robust_optimal_option(
        signal, close, config['NEAREST_LTP'], instruments_df, config, user,
        hedge_offset=h_offset, hedge_required=h_req
    )
    if search_res and search_res[0] is not None:
        result = search_res
        break
    logging.info(f"⚠️ {key} | Search Attempt {attempt+1} failed. Retrying...")
    time.sleep(2)

if result is None:
    logging.error(f"❌ {key} | No valid option found for reentry. Skipping.")
    continue

# Unpack exactly according to your function's return signature
opt_symbol, strike, expiry, main_ltp, hedge_opt_symbol = result

# 2. STATE & EXPIRY EVALUATION
last_expiry = trade['Expiry']
expiry_match = "SAME" if expiry == last_expiry else "DIFF"
target_qty = int(config['QTY'])
existing_qty = int(trade.get('qty', target_qty))
qty_changed = (target_qty != existing_qty)

# Price for exit PnL calculation
current_h_ltp = get_quotes_with_retry(trade.get('hedge_option_symbol'), user)

# 3. EXECUTE ROBUST EXIT (Clears previous position)
exit_qty, exit_avg, exit_h_avg = execute_robust_exit(trade, config, user, expiry_match=expiry_match)
if expiry_match == "SAME" and not qty_changed:
    exit_h_avg = current_h_ltp

if exit_qty > 0 and exit_avg > 0: 
    # Finalize the old trade data
    trade.update({
        "OptionBuyPrice": exit_avg, "SpotExit": close, "ExitTime": current_time,
        "PnL": (trade["OptionSellPrice"] - exit_avg), "qty": exit_qty,
        "ExitReason": "ROLLOVER_EXIT", "hedge_option_sell_price": exit_h_avg,
        "hedge_pnl": (exit_h_avg - trade["hedge_option_buy_price"]) if exit_h_avg > 0 else 0,
        "total_pnl": (trade["OptionSellPrice"] - exit_avg) + (exit_h_avg - trade["hedge_option_buy_price"])
    })
    
    # Record and delete from Open Positions
    record_trade(get_clean_trade(trade), config, user['id'])
    delete_open_position(trade["OptionSymbol"], config, trade, user['id'])
    send_telegram_message(f"📤 {key} | Rollover Exit {signal}", user['telegram_chat_id'], user['telegram_token'])
else:
    logging.error(f"❌ {key} | Exit Failure at Rollover. Aborting.")
    break

# 4. H-P10 SPECIAL OVERRIDE: REPLACES get_hedge_option for P10
if expiry_match == "DIFF" or config['HEDGE_ROLLOVER_TYPE'] == 'FULL':
    if config['HEDGE_TYPE'] == "H-P10":
        for attempt in range(3):
            # Target the price-based strike specifically
            p10_res = get_robust_optimal_option(signal, close, HEDGE_NEAREST_LTP, instruments_df, config, user, hedge_required=False)
            if p10_res and p10_res[0] is not None:
                hedge_opt_symbol = p10_res[0] # The tradingsymbol
                break
            time.sleep(2)

# 5. EXECUTE NEW ENTRY
temp_trade_symbols = {"OptionSymbol": opt_symbol, "hedge_option_symbol": hedge_opt_symbol}

# Preservation logic: Don't enter a new hedge if SEMI + SAME Expiry + Same Qty
skip_h_entry = (config['HEDGE_TYPE'] == "NH") or \
               (config['HEDGE_ROLLOVER_TYPE'] == 'SEMI' and expiry_match == "SAME" and not qty_changed)

new_qty, new_avg, new_h_avg = execute_robust_entry(temp_trade_symbols, config, user, skip_hedge_override=skip_h_entry)

# 6. FINALIZE & SAVE NEW POSITION
if is_valid_trade_data(new_qty, new_avg, new_h_avg, hedge_required=(not skip_h_entry)):
    h_direction = -1 if signal == "BUY" else 1
    new_trade = {
        "Signal": signal, "SpotEntry": close, "OptionSymbol": opt_symbol,
        "Strike": strike, "Expiry": expiry, "OptionSellPrice": new_avg, "EntryTime": current_time,
        "qty": new_qty, "interval": config['INTERVAL'], "real_trade": config['REAL_TRADE'],
        "EntryReason": "ROLLOVER_REENTRY", "Key": key,
        "hedge_option_symbol": hedge_opt_symbol,
        "hedge_strike": strike + (h_offset * h_direction), 
        "hedge_option_buy_price": trade.get('hedge_option_sell_price') if skip_h_entry and config['HEDGE_TYPE'] != "NH" else new_h_avg,
        "hedge_qty": new_qty if config['HEDGE_TYPE'] != "NH" else 0,
        "hedge_entry_time": trade.get('hedge_entry_time') if skip_h_entry else current_time
    }
    
    save_open_position(get_clean_trade(new_trade), config, user['id'])
    logging.info(f"✅ {key} | Rollover Successful: {opt_symbol} @ ₹{new_avg:.2f}")
    send_telegram_message(f"🔁 {key} | Rollover Reentry {signal}\n{opt_symbol} @ ₹{new_avg:.2f}", user['telegram_chat_id'], user['telegram_token'])
