import numpy as np 

all_features = ['oir', 'mpb', 'voi']
levels = 5
arr = np.array([1, 0.8, 0.6, 0.4, 0.2])
sum_arr = arr.sum()
cum_sum_arr = np.cumsum(arr)

def oir(**kwargs):
    prev_data = kwargs['prev_data']
    data = kwargs['data']
    if prev_data is None: return np.nan
    bid_volumes = np.array(data[10:15])
    ask_volumes = np.array(data[30:35])
    vb = (bid_volumes * arr).sum() / sum_arr
    va = (ask_volumes * arr).sum() / sum_arr
    return (vb - va) / (vb + va)

def mpb(**kwargs):
    prev_data = kwargs['prev_data']
    if prev_data is None: return np.nan
    data = kwargs['data']
    vwap = kwargs['vwap']
    midp = (data[0] + data[20]) / 2
    prev_midp = (prev_data[0] + prev_data[20]) / 2
    return vwap - (midp + prev_midp) / 2

def voi(**kwargs):
    prev_data = kwargs['prev_data']
    data = kwargs['data']
    if prev_data is None: return np.nan
    delta_bid = np.zeros(levels)
    delta_ask = np.zeros(levels)
    
    for i in range(levels):
        prev_bid_i = prev_data[i]
        prev_ask_i = prev_data[i + 20]
        bid_i = data[i]
        ask_i = data[i + 20]
        
        if bid_i > prev_bid_i:
            delta_bid[i] = data[10 + i]
        elif bid_i < prev_bid_i:
            delta_bid[i] = 0
        else:
            delta_bid[i] = data[10 + i] - prev_data[10 + i]
        
        if ask_i > prev_ask_i:
            delta_ask[i] = 0
        elif ask_i < prev_ask_i:
            delta_ask[i] = data[30 + i]
        else:
            delta_ask[i] = data[30 + i] - prev_data[30 + i]
        
        delta_bid[i] = delta_bid[i] * arr[i]
        delta_ask[i] = delta_ask[i] * arr[i]
    
    delta_bid = np.cumsum(delta_bid)
    delta_ask = np.cumsum(delta_ask)
    
    return ((delta_bid - delta_ask) / cum_sum_arr).sum()

all_feature_funcs = [oir, mpb, voi]