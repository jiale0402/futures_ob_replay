from datetime import datetime

def check_ob(ob_handler, bid_limits, ask_limits, timestamp):
    consistent = True
    local_ob_bid_prices = ob_handler.bid_prices
    local_ob_bid_volumes = ob_handler.bid_volumes
    local_ob_ask_prices = ob_handler.ask_prices
    local_ob_ask_volumes = ob_handler.ask_volumes    
    timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
    for i in range(len(bid_limits)):
        bid_price_diff = local_ob_bid_prices[i] - bid_limits[i][0]
        bid_volume_diff = local_ob_bid_volumes[i] - bid_limits[i][1]
        if abs(bid_price_diff) > 1e-3 or abs(bid_volume_diff) > 1e-3:
            consistent = False
            msg = f"Timestamp: {timestamp}, Bid mismatch at Level {i}: bid_price_diff: {abs(bid_price_diff)},\
                    bid_volume_diff: {abs(bid_volume_diff)}, reference bid_limits: {bid_limits},\
                    local bid snapshot: {ob_handler.take_snapshot()[0:20]}"
            print(msg)
            break
    for i in range(len(ask_limits)):
        ask_price_diff = local_ob_ask_prices[i] - ask_limits[i][0]
        ask_volume_diff = local_ob_ask_volumes[i] - ask_limits[i][1]
        if abs(ask_price_diff) > 1e-3 or abs(ask_volume_diff) > 1e-3:
            consistent = False
            msg = f"Timestamp: {timestamp}, Ask mismatch at Level {i}: ask_price_diff: {abs(ask_price_diff)},\
                    ask_volume_diff: {abs(ask_volume_diff)}, reference ask_limits: {ask_limits},\
                    local ask snapshot: {ob_handler.take_snapshot()[20:40]}"
            print(msg)
            break
    return consistent
    
