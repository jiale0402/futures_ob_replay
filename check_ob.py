def check_ob(ob_handler, bid_limits, ask_limits):
    consistent = True 
    local_ob_bid_prices = ob_handler.bid_prices
    local_ob_bid_volumes = ob_handler.bid_volumes
    local_ob_ask_prices = ob_handler.ask_prices
    local_ob_ask_volumes = ob_handler.ask_volumes    
    for i in range(min(len(bid_limits), len(ask_limits))):
        bid_price_diff = local_ob_bid_prices[i] - bid_limits[i][0]
        bid_volume_diff = local_ob_bid_volumes[i] - bid_limits[i][1]
        ask_price_diff = local_ob_ask_prices[i] - ask_limits[i][0]
        ask_volume_diff = local_ob_ask_volumes[i] - ask_limits[i][1]
        if abs(bid_price_diff) > 1e-3 or abs(bid_volume_diff) > 1e-3 \
            or abs(ask_price_diff) > 1e-3 or abs(ask_volume_diff) > 1e-3:
            consistent = False
            break
    return consistent
        