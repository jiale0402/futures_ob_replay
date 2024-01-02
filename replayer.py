import os
import gzip
import datetime
import polars as pl 
import orjson as json
from data_schema import L2_SCHEMA, L1_SCHEMA
from orderbook import LocalOrderBook
from trades import TradesHandler
import copy
        
class Replayer:
    
    def __init__(
            self, 
            src: str, 
            eid: str, 
            dest: str,
            start: str = None,
            frequency: datetime.timedelta = datetime.timedelta(milliseconds=100),
            universe: list = [],
            buffer_size: int = 2**20,
        ) -> None:
        """ 
        main thread of the feature generation process
        
        Params:
        -------
        path:           str, absolute path to the directory containing the data
        eid:            str, exchange id
        dest:           str, absolute path to the directory to store the output
        start:          str, start date of the data to be replayed, format: YYYY-MM-DD
        frequency:      timedelta, frequency of data replay
        universe:       list, string codes of all instruments, if None, will be inferred from data
        buffer_size:    int, buffer size of the output file streams
        """
        self.start = start
        self.time = None
        self.curr_data = {}
        self.dir = src
        self.dates = self.list_dates(self.dir)
        self.eid = eid
        self.freq = frequency
        self.l2_col_mapping = None
        self.universe = universe
        self.ob_container = dict()
        self.dest = dest
        self.buffer_size = buffer_size
        os.makedirs(self.dest, exist_ok=True)
        self.blank_update_template = {k: None for k in L2_SCHEMA.keys()}
        self.blank_trade_template = {k: None for k in L1_SCHEMA.keys()}
        self.blank_trade_template['Code'] = 'blank'
        self.blank_update_template['Code'] = 'blank'
    
    def compute_day(self):
        """
        computes one day worth of features and write to destination
        """
        self._read_next_date()
        
        # replay loop
        for (l2_updates, trades) in zip(
            self.curr_data['l2'].iter_rows(named = True),
            self.curr_data['trades'].iter_rows(named = True)
        ):
            
            # assure time is uniform
            timestamp = l2_updates.pop('Timestamp')
            assert timestamp == trades.pop('Timestamp')
            
            # process l2 updates
            if l2_updates['Code'] is not None:
                for row in zip(*l2_updates.values()):
                    self.handle_l2_update(row)
            
            # process trades 
            if trades['Code'] is not None:
                for row in zip(*trades.values()):
                    if row[self.l1_col_mapping['Code']] == 'blank':
                        continue
                    self.handle_trades(row)
            
            # record the features
            for k, v in self.dest_file_streams.items():
                data = self.ob_container[k].take_snapshot()
                data += self.trade_handler_container[k].get_ohlcva()
                v.write(f"{str(data)[1:-1]}, {timestamp}\n")
                    
    #######################     
    ### handler methods ###
    #######################
    def handle_trades(self, row) -> None: # message handler wrapper
        code = row[self.l1_col_mapping['Code']]
        price = row[self.l1_col_mapping['TradeEvent_LastPrice']]
        qty = row[self.l1_col_mapping['TradeEvent_LastTradeQuantity']]
        self.trade_handler_container[code].handle_trades(price, qty)
    
    def handle_l2_update(self, row) -> None: # message handler wrapper
        code = row[self.l2_col_mapping['Code']]
        # 1.4.4.8   OverlapRefresh
        if row[self.l2_col_mapping['OverlapRefresh_BidChangeIndicator']] is not None or\
           row[self.l2_col_mapping['OverlapRefresh_AskChangeIndicator']] is not None:
            self.handle_OverlapRefresh(row, self.ob_container[code])
        # 1.4.2     DeltaRefresh
        elif row[self.l2_col_mapping['DeltaRefresh_DeltaAction']] is not None:
            self.handle_DeltaRefresh(row, self.ob_container[code])
        # 1.4.4.9   MBLMaxVisibleDepth
        elif row[self.l2_col_mapping['MaxVisibleDepth_MaxVisibleDepth']] is not None:
            code = row[self.l2_col_mapping['Code']]
            self.handle_MBLMaxVisibleDepth(row, self.ob_container[code])

    def handle_MBLMaxVisibleDepth(self, row, ob):
        depth = row[self.l2_col_mapping['MaxVisibleDepth_MaxVisibleDepth']]
        ob.MaxVisibleDepth(int(depth))
    
    def handle_OverlapRefresh_indicator(self, indicator):
        # decode start level 1.4.1.3
        if indicator < 0:
            is_full = True
            start_level = -indicator-1
        else:
            is_full = False
            start_level = indicator
        return is_full, int(start_level)
    
    def handle_OverlapRefresh(self, row, ob):
        # process a partial or full order book snapshot
        bid_indicator = row[self.l2_col_mapping['OverlapRefresh_BidChangeIndicator']]
        ask_indicator = row[self.l2_col_mapping['OverlapRefresh_AskChangeIndicator']]
        _, bid_start_level = self.handle_OverlapRefresh_indicator(bid_indicator)
        _, ask_start_level = self.handle_OverlapRefresh_indicator(ask_indicator)
        bid_limits = row[self.l2_col_mapping['OverlapRefresh_BidLimits']]
        ask_limits = row[self.l2_col_mapping['OverlapRefresh_AskLimits']]
        if bid_limits is not None: # load bid limits (snapshot)
            if ob.bid_prices[0] is None:
                bid_start_level = 0
            ob.BidClearFromLevel(bid_start_level)
            bid_limits = json.loads(f"[{bid_limits}]".replace('][', '],['))
            for i in range(len(bid_limits)):
                ob.BidInsertAtLevel(bid_start_level+i, bid_limits[i][0], bid_limits[i][1])
        if ask_limits is not None: # load ask limits (snapshot)
            if ob.ask_prices[0] is None:
                ask_start_level = 0
            ob.AskClearFromLevel(ask_start_level)   
            ask_limits = json.loads(f"[{ask_limits}]".replace('][', '],['))
            for i in range(len(ask_limits)):
                ob.AskInsertAtLevel(ask_start_level+i, ask_limits[i][0], ask_limits[i][1])
                
    def handle_DeltaRefresh(self, row, ob):
        # process a delta update
        action = row[self.l2_col_mapping['DeltaRefresh_DeltaAction']]
        level = int(row[self.l2_col_mapping['DeltaRefresh_Level']])
        if row[self.l2_col_mapping['DeltaRefresh_Price']] is not None:
            assert row[self.l2_col_mapping['DeltaRefresh_Price']] >= 0, f"price is negative: {row, row[self.l2_col_mapping['DeltaRefresh_Price']]}"
        if action == "0.0":     # 1.4.2 0 - ALLClearFromLevel	
            ob.ALLClearFromLevel(level)
        elif action == "1.0":   # 1.4.2 1 - BidClearFromLevel
            ob.BidClearFromLevel(level)
        elif action == "2.0":   # 1.4.2 2 - AskClearFromLevel
            ob.AskClearFromLevel(level)
        elif action == "3.0":   # 1.4.2 3 - BidInsertAtLevel
            price = row[self.l2_col_mapping['DeltaRefresh_Price']]
            qty = row[self.l2_col_mapping['DeltaRefresh_CumulatedUnits']]
            ob.BidInsertAtLevel(level, price, qty)
        elif action == "4.0":   # 1.4.2 4 - AskInsertAtLevel
            price = row[self.l2_col_mapping['DeltaRefresh_Price']]
            qty = row[self.l2_col_mapping['DeltaRefresh_CumulatedUnits']]
            ob.AskInsertAtLevel(level, price, qty)
        elif action == "5.0":   # 1.4.2 5 - BidRemoveLevel
            ob.BidRemoveLevel(level)
        elif action == "6.0":   # 1.4.2 6 - AskRemoveLevel
            ob.AskRemoveLevel(level)
        elif action == "7.0":   # 1.4.2 7 - BidChangeQtyAtLevel
            qty = row[self.l2_col_mapping['DeltaRefresh_CumulatedUnits']]
            ob.BidChangeQtyAtLevel(level, qty)
        elif action == "8.0":   # 1.4.2 8 - AskChangeQtyAtLevel
            qty = row[self.l2_col_mapping['DeltaRefresh_CumulatedUnits']]
            ob.AskChangeQtyAtLevel(level, qty)
        elif action == "9.0":   # 1.4.2 9 - BidRemoveLevelAndAppend
            price = row[self.l2_col_mapping['DeltaRefresh_Price']]
            qty = row[self.l2_col_mapping['DeltaRefresh_CumulatedUnits']]
            ob.BidRemoveLevelAndAppend(level, price, qty)
        elif action == "10.0":  # 1.4.2 10 - AskRemoveLevelAndAppend
            price = row[self.l2_col_mapping['DeltaRefresh_Price']]
            qty = row[self.l2_col_mapping['DeltaRefresh_CumulatedUnits']]
            ob.AskRemoveLevelAndAppend(level, price, qty)
        
    def _read_next_date(self) -> None:
        # read next date's data
        try:
            self.date = self.dates.pop(0)
        except:
            raise ValueError("No more data to be replayed")
        date, dir, eid = self.date, self.dir, self.eid
        
        # load l2 data
        self.curr_data.clear()
        self.curr_data['date'] = date
        self.curr_data['l2'] =  pl.read_csv(
            gzip.open(os.path.join(dir, f"{date}_{eid}_L2.csv.gz"), 'rb').read(), 
            schema=L2_SCHEMA
        )
                         
        # load l1 data
        self.curr_data['trades'] = pl.read_csv(
            gzip.open(os.path.join(dir, "trades", f"{date}_{eid}_L1-Trades.csv.gz"), 'rb').read(),
            schema=L1_SCHEMA
        )
        
        # filter out instruments not in the universe
        if self.universe != []:
            self.curr_data['l2'] = self.curr_data['l2'].filter(
                pl.col('Code').is_in(self.universe) | pl.col('Code').eq('blank')
            )
            self.curr_data['trades'] = self.curr_data['trades'].filter(
                pl.col('Code').is_in(self.universe) | pl.col('Code').eq('blank')
            )
        
        # cast the string timestamps to datetime objects
        self.curr_data['l2'] = self.curr_data['l2'].with_columns(
            Timestamp = pl.coalesce(
            ['DeltaRefresh_ServerTimestamp', 
             'OverlapRefresh_ServerTimestamp'])
            .str.split(".")
            .list
            .first()
            .cast(pl.Int64)
            .cast(pl.Datetime)
            .dt.with_time_unit("us")
            .backward_fill() # backward fill to fill the None values in timestamps
            # Note: we can do it because only max_visible_depth messages have None timestamps
            # and they do not affect the order book
        )
        self.curr_data['trades'] = self.curr_data['trades'].with_columns(
            pl.col('ServerTimestamp').cast(pl.Int64)
            .cast(pl.Datetime)
            .dt.with_time_unit("us")
            .alias("Timestamp")
        )
        
        # insert a blank message to end of trades/l2 data 
        # to ensure uniform sampling
        l2_schema = copy.deepcopy(L2_SCHEMA)
        l2_schema['Timestamp'] = pl.Datetime
        l1_schema = copy.deepcopy(L1_SCHEMA)
        l1_schema['Timestamp'] = pl.Datetime
        self.time = datetime.datetime.strptime(date, "%Y-%m-%d")
        blank_update = copy.deepcopy(self.blank_update_template)
        blank_update['Timestamp'] = self.time + datetime.timedelta(hours=22) - self.freq / 2
        blank_trade = copy.deepcopy(self.blank_trade_template)
        blank_trade['Timestamp'] = self.time + datetime.timedelta(hours=22) - self.freq / 2
        self.curr_data['l2'] = pl.concat([
            self.curr_data['l2'],
            pl.DataFrame(blank_update, schema_overrides=l2_schema),
        ])
        self.curr_data['trades'] = pl.concat([
            self.curr_data['trades'],
            pl.DataFrame(blank_trade, schema_overrides=l1_schema),
        ])
        
        # initialize universe and column mapping on first date
        if len(self.ob_container) == 0:
            self.init_params()

        # insert a blank message to start of trades/l2 data 
        blank_update['Timestamp'] = self.time 
        blank_trade['Timestamp'] = self.time
        self.curr_data['l2'] = pl.concat([
            pl.DataFrame(blank_update, schema_overrides=l2_schema),
            self.curr_data['l2'],
        ])
        self.curr_data['trades'] = pl.concat([
            pl.DataFrame(blank_trade, schema_overrides=l1_schema),
            self.curr_data['trades'],
        ])
        
        # aggregating and upsampling to make the data time uniform
        # each row in the dataframe is a collection of messages that happened in this interval
        self.curr_data['l2'] = self.curr_data['l2'].filter(
                pl.col('Code').is_in(self.universe) | pl.col('Code').eq('blank')
            ).set_sorted(
                'Timestamp',
                descending=False
            ).filter(
                (pl.col('Timestamp') >= self.time) & 
                (pl.col('Timestamp') < self.time + datetime.timedelta(days=1))
            ).group_by_dynamic(
                index_column="Timestamp",
                every=self.freq,
                include_boundaries=True,
                closed='left',
            ).agg(
                pl.col('Code'),
                pl.col('OverlapRefresh_BidChangeIndicator'),
                pl.col('OverlapRefresh_AskChangeIndicator'),
                pl.col('OverlapRefresh_BidLimits'),
                pl.col('OverlapRefresh_AskLimits'),
                pl.col('MaxVisibleDepth_MaxVisibleDepth'),
                pl.col('DeltaRefresh_DeltaAction'),
                pl.col('DeltaRefresh_CumulatedUnits'),
                pl.col('DeltaRefresh_Level'),
                pl.col('DeltaRefresh_Price'),
            ).select(
                pl.col('Code'),
                pl.col('OverlapRefresh_BidChangeIndicator'),
                pl.col('OverlapRefresh_AskChangeIndicator'),
                pl.col('OverlapRefresh_BidLimits'),
                pl.col('OverlapRefresh_AskLimits'),
                pl.col('MaxVisibleDepth_MaxVisibleDepth'),
                pl.col('DeltaRefresh_DeltaAction'),
                pl.col('DeltaRefresh_CumulatedUnits'),
                pl.col('DeltaRefresh_Level'),
                pl.col('DeltaRefresh_Price'),
                pl.col("_upper_boundary").alias("Timestamp"),
            ).upsample(
                time_column="Timestamp",
                every=self.freq
            )
        self.curr_data['trades'] = self.curr_data['trades'].filter(
                (pl.col('Code').is_in(self.universe) | pl.col('Code').eq('blank'))
            ).set_sorted(
                'Timestamp', 
                descending=False
            ).filter(
                (pl.col('Timestamp') >= self.time) & 
                (pl.col('Timestamp') < self.time + datetime.timedelta(days=1))
            ).group_by_dynamic(
                index_column="Timestamp",
                every=self.freq,
                include_boundaries=True,
                closed='left',
            ).agg(
                pl.col("TradeEvent_LastPrice"),
                pl.col("TradeEvent_LastTradeQuantity"),
                pl.col("Code"),
            ).select(
                pl.col("_upper_boundary").alias("Timestamp"),
                pl.col("TradeEvent_LastPrice"),
                pl.col("TradeEvent_LastTradeQuantity"),
                pl.col("Code")
            ).upsample(
                time_column="Timestamp",
                every=self.freq
            )
            
        if self.l2_col_mapping is None:
            self.l2_col_mapping = {col: i for i, col in enumerate(self.curr_data['l2'].columns[1:])}
            self.l1_col_mapping = {col: i for i, col in enumerate(self.curr_data['trades'].columns[1:])}
            # the first column is the timestamp, which is not in the schema
            
        # sanity check
        msg = f"l2 and l1 dataframes have different shapes: {self.curr_data['l2'].shape[0]}, {self.curr_data['trades'].shape[0]}"
        assert self.curr_data['l2'].shape[0] == self.curr_data['trades'].shape[0], msg
        
    def __repr__(self) -> str:
        return f"Replayer({self.dir}, {self.eid}, {self.freq})"
    
    def init_params(self):
        if self.universe == []:
            self.universe = self.curr_data['l2']['Code'].unique().to_list()
            self.universe = [code for code in self.universe if code != 'blank']
        self.ob_container = {code: LocalOrderBook(code) for code in self.universe}
        self.trade_handler_container = {code: TradesHandler(code, self.freq) for code in self.universe}
        self.time = self.curr_data['l2']['Timestamp'].min().replace(hour=22, minute=0, second=0, microsecond=0)
        
        self.dest_file_streams = {code: open(os.path.join(self.dest, f"{code}.csv"), 'w+', buffering=self.buffer_size) for code in self.universe}
        features = [f'bid_price_{i}' for i in range(10)] + [f'bid_qty_{i}' for i in range(10)] +\
                   [f'ask_price_{i}' for i in range(10)] + [f'ask_qty_{i}' for i in range(10)] +\
                   ['open', 'high', 'low', 'close', 'volume', 'amount', 'timestamp']
        features = ', '.join(features)
        for dest in self.dest_file_streams.values():
            dest.write(f"{features}\n")
    
    def list_dates(self, dir) -> list:
        assert os.path.isdir(dir), f"{dir} is not a directory"
        dates = set()
        for file in os.listdir(dir):
            if file.endswith(".csv.gz"):
                dates.add(file.split('_')[0])
        self.time = datetime.datetime.strptime(min(dates), "%Y-%m-%d")
        dates = sorted(list(dates))
        dates = [d for d in dates if d >= self.start]
        return dates
            