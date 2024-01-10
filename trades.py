import numpy as np 

class TradesHandler:
    
    """
    should be used to store trade data for a single instrument
    has handler functions for all types of trade updates
    """
    
    def __init__(self, code, freq):
        self.code = code
        self.freq = freq
        self.recent_trade_prices = []
        self.recent_trade_volumes = []
        self.prev_open = None
        self.prev_high = None
        self.prev_low = None
        self.prev_close = None
        self.vwap = np.nan
        
    def handle_trades(self, price, qty):
        if price is None: return
        price = round(price, 5)
        qty = round(qty, 5)
        self.recent_trade_prices.append(price)
        self.recent_trade_volumes.append(qty)
    
    def get_ohlcva(self):
        if len(self.recent_trade_prices) == 0:
            # open, high, low, close, volume, amount
            return [self.prev_close] * 4 + [0] * 2 
        else:
            self.prev_open = self.recent_trade_prices[0]
            self.prev_high = max(self.recent_trade_prices)
            self.prev_low = min(self.recent_trade_prices)
            self.prev_close = self.recent_trade_prices[-1]
            volume = sum(self.recent_trade_volumes)
            amount = sum([p * v for p, v in zip(self.recent_trade_prices, self.recent_trade_volumes)])
            if volume != 0: # update vwap
                self.prev_vwap = amount / volume
            self.recent_trade_prices = []
            self.recent_trade_volumes = []
            return self.prev_open, self.prev_high, self.prev_low, self.prev_close, volume, amount
        
