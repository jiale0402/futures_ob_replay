import numpy as np
import polars as pl
import orjson as json

from trades import TradesHandler
from orderbook import LocalOrderBook
from feature_func import all_feature_funcs
from check_ob import check_ob

def compute_day(
        l2: pl.DataFrame,
        l1: pl.DataFrame,
        l2_col_mapping: dict, 
        l1_col_mapping: dict, 
        ob_handler: LocalOrderBook, 
        trade_handler: TradesHandler, 
        dest: str,
        buffer_size: int = 2**20,
        last = None
    ) -> tuple:    
    dest = open(dest, 'a', buffering=buffer_size) 
    # replay loop
    prev_data = last
    overlaprefresh_check_results = []
    for (l2_updates, trades) in zip(l2.iter_rows(named = True), l1.iter_rows(named = True)):
        # assure time is uniform
        timestamp = l2_updates.pop('Timestamp')
        assert timestamp == trades.pop('Timestamp')
        
        # process l2 updates
        if l2_updates['Code'] is not None:
            for row in zip(*l2_updates.values()):
                layer = row[l2_col_mapping['LayerId']]
                if layer is None:
                    continue
                res, bid_limits, ask_limits = handle_l2_update(row, l2_col_mapping, ob_handler[layer])
                # log correctness check results
                if res is not None and layer == "0":
                    overlaprefresh_check_results.append((res, timestamp, layer, bid_limits, ask_limits, ob_handler[layer].take_snapshot()))
        
        # process trades 
        if trades['Code'] is not None:
            for row in zip(*trades.values()):
                handle_trades(row, l1_col_mapping, trade_handler)
        
        # record the features
        data = [col for ob_container in ob_handler.values() for col in ob_container.take_snapshot()]
        data += trade_handler.get_ohlcva()
        data += [f(
                    data=data, 
                    prev_data=prev_data, 
                    vwap=trade_handler.vwap
                ) for f in all_feature_funcs]
        dest.write(f"{str(data)[1:-1]}, {timestamp}\n")
        prev_data = data

    dest.close()
    if len(overlaprefresh_check_results) == 0:
        accuracy = np.nan
    else:
        r = []
        for res, timestamp, layer, bid_limits, ask_limits, snapshot in overlaprefresh_check_results:
            print(f"OverlapRefresh check failed at {timestamp} in layer {layer}")
            print(f"bid_limits {bid_limits} and ask_limits {ask_limits}")
            print(f"Snapshot: {snapshot}")
            r.append(res)
        accuracy = np.sum(r) / len(r)
        
    return data, accuracy

def handle_trades(row, l1_col_mapping, trades_handler) -> None: # message handler wrapper
    price = row[l1_col_mapping['TradeEvent_LastPrice']]
    qty = row[l1_col_mapping['TradeEvent_LastTradeQuantity']]
    trades_handler.handle_trades(price, qty)

def handle_l2_update(row, l2_col_mapping, ob_handler) -> None: # message handler wrapper
    res = None # place holder for overlap refresh reference check result
    # 1.4.4.8   OverlapRefresh
    bid_limits, ask_limits = None, None
    if row[l2_col_mapping['OverlapRefresh_BidChangeIndicator']] is not None or\
       row[l2_col_mapping['OverlapRefresh_AskChangeIndicator']] is not None:
        res, bid_limits, ask_limits = handle_OverlapRefresh(row, ob_handler, l2_col_mapping)
    # 1.4.2     DeltaRefresh
    elif row[l2_col_mapping['DeltaRefresh_DeltaAction']] is not None:
        handle_DeltaRefresh(row, ob_handler, l2_col_mapping)
    # 1.4.4.9   MBLMaxVisibleDepth
    elif row[l2_col_mapping['MaxVisibleDepth_MaxVisibleDepth']] is not None:
        handle_MBLMaxVisibleDepth(row, ob_handler, l2_col_mapping)
    return res, bid_limits, ask_limits

def handle_MBLMaxVisibleDepth(row, ob, l2_col_mapping):
    depth = row[l2_col_mapping['MaxVisibleDepth_MaxVisibleDepth']]
    ob.MaxVisibleDepth(int(depth))

def handle_OverlapRefresh_indicator(indicator):
    # decode start level 1.4.1.3
    if indicator < 0:
        is_full = True
        start_level = -indicator-1
    else:
        is_full = False
        start_level = indicator
    return is_full, int(start_level)

def handle_OverlapRefresh(row, ob, l2_col_mapping):
    # process a partial or full order book snapshot
    bid_indicator = row[l2_col_mapping['OverlapRefresh_BidChangeIndicator']]
    ask_indicator = row[l2_col_mapping['OverlapRefresh_AskChangeIndicator']]
    bid_is_full, bid_start_level = handle_OverlapRefresh_indicator(bid_indicator)
    ask_is_full, ask_start_level = handle_OverlapRefresh_indicator(ask_indicator)
    bid_limits = row[l2_col_mapping['OverlapRefresh_BidLimits']]
    ask_limits = row[l2_col_mapping['OverlapRefresh_AskLimits']]
    
    if bid_limits is not None: # load bid limits (snapshot)
        if ob.bid_prices[0] is None:
            bid_start_level = 0
        bid_limits = json.loads(f"[{bid_limits}]".replace('][', '],['))
        for i in range(len(bid_limits)):
            ob.BidOverwriteLevel(bid_limits[i][0], bid_limits[i][1], bid_start_level+i)
        if bid_is_full:
            ob.BidClearFromLevel(i + 1)
    if ask_limits is not None: # load ask limits (snapshot)
        if ob.ask_prices[0] is None:
            ask_start_level = 0
        if ask_is_full:
            ob.AskClearFromLevel(0)
        ask_limits = json.loads(f"[{ask_limits}]".replace('][', '],['))
        for i in range(len(ask_limits)):
            ob.AskOverwriteLevel(ask_limits[i][0], ask_limits[i][1], ask_start_level+i)
        if ask_is_full:
            ob.AskClearFromLevel(i + 1)
            
    # this is a full snapshot, check for local ob accuracy
    if (bid_is_full and ask_is_full) and (bid_limits and ask_limits): 
        res = check_ob(ob, bid_limits, ask_limits)
    else:
        res = None
    return res, bid_limits, ask_limits

def handle_DeltaRefresh(row, ob, l2_col_mapping):
    # process a delta update
    action = row[l2_col_mapping['DeltaRefresh_DeltaAction']]
    level = int(row[l2_col_mapping['DeltaRefresh_Level']])
    price = row[l2_col_mapping['DeltaRefresh_Price']]
    qty = row[l2_col_mapping['DeltaRefresh_CumulatedUnits']]
    if action == "0.0":     # 1.4.2 0 - ALLClearFromLevel
        ob.ALLClearFromLevel(level)
    elif action == "1.0":   # 1.4.2 1 - BidClearFromLevel
        ob.BidClearFromLevel(level)
    elif action == "2.0":   # 1.4.2 2 - AskClearFromLevel
        ob.AskClearFromLevel(level)
    elif action == "3.0":   # 1.4.2 3 - BidInsertAtLevel
        ob.BidInsertAtLevel(level, price, qty)
    elif action == "4.0":   # 1.4.2 4 - AskInsertAtLevel
        ob.AskInsertAtLevel(level, price, qty)
    elif action == "5.0":   # 1.4.2 5 - BidRemoveLevel
        ob.BidRemoveLevel(level)
    elif action == "6.0":   # 1.4.2 6 - AskRemoveLevel
        ob.AskRemoveLevel(level)
    elif action == "7.0":   # 1.4.2 7 - BidChangeQtyAtLevel
        ob.BidChangeQtyAtLevel(level, qty)
    elif action == "8.0":   # 1.4.2 8 - AskChangeQtyAtLevel
        ob.AskChangeQtyAtLevel(level, qty)
    elif action == "9.0":   # 1.4.2 9 - BidRemoveLevelAndAppend
        ob.BidRemoveLevelAndAppend(level, price, qty)
    elif action == "10.0":  # 1.4.2 10 - AskRemoveLevelAndAppend
        ob.AskRemoveLevelAndAppend(level, price, qty)
