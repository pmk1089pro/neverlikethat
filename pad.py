# --- REENTRY / ROLLOVER LOGIC ---
result = (None, None, None, None, None)
signal = trade["Signal"]

for attempt in range(3):
    result = get_robust_optimal_option(
        signal,
        close,
        config['NEAREST_LTP'],
        instruments_df,
        config,
        user,
        hedge_offset=config.get('HEDGE_OFFSET', 200),
        hedge_required=(config['HEDGE_TYPE'] != "NH")
    )

    # If valid symbol found
    if result and result[0] is not None:
        break

    logging.info(f"⚠️{key} | Search Attempt {attempt+1} failed. Retrying in 2s...")
    time.sleep(2)

last_expiry = trade['Expiry']

if not result or result[0] is None:
    logging.error(f"❌ {key} | No expiry found after {last_expiry} for reentry.")
    position = None
    continue

# ✅ NEW unpacking (includes hedge symbol)
opt_symbol, strike, expiry, ltp, hedge_opt_symbol = result
main_ltp = ltp

# 1. Determine Expiry Match
expiry_match = "SAME" if expiry == last_expiry else "DIFF"

# 2. Qty Logic
target_qty = int(config['QTY'])
existing_qty = int(trade.get('qty', target_qty))
qty_changed = (target_qty != existing_qty)

# Existing hedge data
old_trade = trade.copy()

hedge_strike = old_trade.get('hedge_strike')
hedge_expiry = old_trade.get('expiry')

hedge_symbol_old = old_trade.get('hedge_option_symbol')
hedge_ltp = get_quotes_with_retry(hedge_symbol_old, user) if hedge_symbol_old else 0

# 3. EXECUTE ROBUST EXIT
exit_qty, exit_avg, exit_h_avg = execute_robust_exit(
    trade,
    config,
    user,
    expiry_match=expiry_match
)

# SAME expiry + no qty change → keep hedge live price
if expiry_match == "SAME" and not qty_changed and exit_h_avg == 0:
    exit_h_avg = hedge_ltp

if exit_qty > 0 and exit_avg > 0 and exit_h_avg >= 0:

    trade.update({
        "OptionBuyPrice": exit_avg,
        "SpotExit": close,
        "ExitTime": current_time,
        "PnL": (trade["OptionSellPrice"] - exit_avg),
        "qty": exit_qty,
        "ExitReason": "TARGET_HIT",

        "hedge_option_sell_price": exit_h_avg,
        "hedge_exit_time": current_time,
        "hedge_pnl": (exit_h_avg - trade["hedge_option_buy_price"]) if exit_h_avg > 0 else 0,

        "total_pnl": (trade["OptionSellPrice"] - exit_avg) +
                     ((exit_h_avg - trade["hedge_option_buy_price"]) if exit_h_avg > 0 else 0)
    })

else:
    reason = f"{user['user']} | {key} | Exit error: qty={exit_qty}, avg={exit_avg}, hedge_avg={exit_h_avg}"
    logging.error(reason)
    send_telegram_message_admin(reason)
    update_trade_config_on_failure(config['KEY'], reason, user)
    position = None
    continue

# DATABASE ACTIONS
trade = get_clean_trade(trade)
record_trade(trade, config, user['id'])
delete_open_position(trade["OptionSymbol"], config, trade, user['id'])

send_telegram_message(
    f"📤 {key} | {user['user']} {SERVER} | Rollover Exit {trade['Signal']}",
    user['telegram_chat_id'],
    user['telegram_token']
)

# ---------------------------------------------------
# 🔁 HEDGE HANDLING (NO LOGIC CHANGE, JUST SOURCE UPDATED)
# ---------------------------------------------------

# If new hedge required
if expiry_match == "DIFF" or config['HEDGE_ROLLOVER_TYPE'] == 'FULL':

    # Prefer hedge from robust function
    if hedge_opt_symbol is None and config['HEDGE_TYPE'] != "NH":
        logging.error(f"❌ {key} | Hedge not found in robust selection")
        position = None
        continue

else:
    # SAME expiry + SEMI → reuse old hedge
    hedge_opt_symbol = hedge_symbol_old

# ---------------------------------------------------
# 4. PREPARE ENTRY
# ---------------------------------------------------

temp_trade_symbols = {
    "OptionSymbol": opt_symbol,
    "hedge_option_symbol": hedge_opt_symbol
}

# Skip hedge entry logic
skip_h_entry = False

if config['HEDGE_TYPE'] == "NH":
    skip_h_entry = True

elif config['HEDGE_ROLLOVER_TYPE'] == 'SEMI' and expiry_match == "SAME" and not qty_changed:
    skip_h_entry = True

# ---------------------------------------------------
# 5. EXECUTE ENTRY
# ---------------------------------------------------

new_qty, new_avg, new_h_avg = execute_robust_entry(
    temp_trade_symbols,
    config,
    user,
    skip_hedge_override=skip_h_entry
)

logging.info(
    f"📤{key} | Entered {opt_symbol} @ ₹{new_avg:.2f} Qty: {new_qty} | "
    f"Hedge {hedge_opt_symbol} @ ₹{new_h_avg:.2f}"
)

# ✅ FIXED validation bug (new_qty instead of qty)
if not is_valid_trade_data(new_qty, new_avg, new_h_avg, hedge_required=True):
    err_msg = f"⚠️ {key} | FAILED Entry: Qty ({new_qty}) or Price ({new_avg}) or Hedge ({new_h_avg}) invalid"
    logging.error(err_msg)
    send_telegram_message_admin(err_msg)
    position = None
    continue

# ---------------------------------------------------
# 6. FINALIZE TRADE OBJECT
# ---------------------------------------------------

trade = {
    "Signal": signal,
    "SpotEntry": close,
    "OptionSymbol": opt_symbol,
    "Strike": strike,
    "Expiry": expiry,

    "OptionSellPrice": new_avg,
    "EntryTime": current_time,
    "qty": new_qty,

    "interval": config['INTERVAL'],
    "real_trade": config['REAL_TRADE'],
    "EntryReason": "ROLLOVER_REENTRY",
    "Key": key,

    "hedge_option_symbol": hedge_opt_symbol,
    "hedge_strike": hedge_strike,

    "hedge_option_buy_price":
        old_trade.get('hedge_option_sell_price')
        if skip_h_entry and config['HEDGE_TYPE'] != "NH"
        else new_h_avg,

    "hedge_qty": new_qty if config['HEDGE_TYPE'] != "NH" else 0,

    "hedge_entry_time":
        old_trade.get('hedge_entry_time')
        if skip_h_entry else current_time
}

trade = get_clean_trade(trade)
save_open_position(trade, config, user['id'])

logging.info(f"✅ {key} | Rollover/Reentry Complete Qty: {new_qty}")

logging.info(
    f"🔁 {key} | {user['user']} {SERVER} | Rollover Reentry {signal}\n"
    f"{opt_symbol} @ ₹{new_avg:.2f} Qty: {new_qty} | "
    f"Hedge {hedge_opt_symbol} @ ₹{trade['hedge_option_buy_price']:.2f}"
)

send_telegram_message(
    f"🔁 {key} | {user['user']} {SERVER} | Rollover Reentry {signal}\n"
    f"{opt_symbol} @ ₹{new_avg:.2f} Qty: {new_qty} | "
    f"Hedge {hedge_opt_symbol} @ ₹{trade['hedge_option_buy_price']:.2f}",
    user['telegram_chat_id'],
    user['telegram_token']
)

# --- Rollover End ---
