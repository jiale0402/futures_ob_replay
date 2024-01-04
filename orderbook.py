class LocalOrderBook:
    
    """ 
    should be used to store order book data for a single instrument
    has handler functions for all types order book updates
    """
    
    def __init__(self, code:str, initial_levels=15) -> None:
        self.code = code
        self.bid_prices = [None] * initial_levels # best bid at index 0
        self.bid_volumes = [None] * initial_levels
        self.ask_prices = [None] * initial_levels # best ask at index 0
        self.ask_volumes = [None] * initial_levels
        
    def take_snapshot(self, levels=10, mode = 'list'):
        if mode == 'dict':
            snapshot = {}
            snapshot['bid_prices'] = self.bid_prices[:levels]
            snapshot['bid_volumes'] = self.bid_volumes[:levels]
            snapshot['ask_price'] = self.ask_prices[:levels]
            snapshot['ask_volumes'] = self.ask_volumes[:levels]
        else:
            snapshot = self.bid_prices[:levels] + self.bid_volumes[:levels] + self.ask_prices[:levels] + self.ask_volumes[:levels]
        return snapshot
                
    def BidChangeQtyAtLevel(self, level, qty):
        self.bid_volumes[level] = qty
    
    def AskChangeQtyAtLevel(self, level, qty):
        self.ask_volumes[level] = qty
        
    def BidRemoveLevel(self, level):
        self.bid_prices.pop(level)
        self.bid_volumes.pop(level)
        self.bid_prices.append(None)
        self.bid_volumes.append(None)
    
    def AskRemoveLevel(self, level):
        self.ask_prices.pop(level)
        self.ask_volumes.pop(level)
        self.ask_prices.append(None)
        self.ask_volumes.append(None)
        
    def BidInsertAtLevel(self, level, price, qty):
        self.bid_prices.insert(level, price)
        self.bid_volumes.insert(level, qty)
        self.bid_prices.pop(-1)
        self.bid_volumes.pop(-1)
        
    def AskInsertAtLevel(self, level, price, qty):
        self.ask_prices.insert(level, price)
        self.ask_volumes.insert(level, qty)
        self.ask_prices.pop(-1)
        self.ask_volumes.pop(-1)
        
    def BidRemoveLevelAndAppend(self, level, price, qty):
        self.bid_prices.pop(level)
        self.bid_volumes.pop(level)
        self.bid_prices.append(price)
        self.bid_volumes.append(qty)
        
    def AskRemoveLevelAndAppend(self, level, price, qty):
        self.ask_prices.pop(level)
        self.ask_volumes.pop(level)
        self.ask_prices.append(price)
        self.ask_volumes.append(qty)
    
    def BidClearFromLevel(self, level):
        self.bid_prices[level:] = [None] * (len(self.bid_prices) - level)
        self.bid_volumes[level:] = [None] * (len(self.bid_volumes) - level)
    
    def AskClearFromLevel(self, level):
        self.ask_prices[level:] = [None] * (len(self.ask_prices) - level)
        self.ask_volumes[level:] = [None] * (len(self.ask_volumes) - level)
        
    def ALLClearFromLevel(self, level):
        self.BidClearFromLevel(level)
        self.AskClearFromLevel(level)
    
    def MaxVisibleDepth(self, depth):
        if depth < len(self.bid_prices):
            self.bid_prices = self.bid_prices[:depth]
            self.bid_volumes = self.bid_volumes[:depth]
            self.ask_prices = self.ask_prices[:depth]
            self.ask_volumes = self.ask_volumes[:depth]
        elif depth > len(self.bid_prices):
            self.bid_prices += [None] * (depth - len(self.bid_prices))
            self.bid_volumes += [None] * (depth - len(self.bid_volumes))
            self.ask_prices += [None] * (depth - len(self.ask_prices))
            self.ask_volumes += [None] * (depth - len(self.ask_volumes))
    
    def BidOverwriteLevel(self, price, qty, level):
        self.bid_prices[level] = price
        self.bid_volumes[level] = qty

    def AskOverwriteLevel(self, price, qty, level):
        self.ask_prices[level] = price
        self.ask_volumes[level] = qty

    def __repr__(self) -> str:
        return f"Instrument Code: {self.code}" +\
               f"\nbid prices: {self.bid_prices}" +\
               f"\nbid volumes: {self.bid_volumes}" +\
               f"\nask prices: {self.ask_prices}" +\
               f"\nask volumes: {self.ask_volumes}"
