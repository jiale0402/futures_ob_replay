import os
import io
import gzip
import datetime
import polars as pl 
import orjson as json
from data_schema import L2_SCHEMA, L1_SCHEMA
from orderbook import LocalOrderBook
import copy
from trades import TradesHandler
from concurrent.futures import ProcessPoolExecutor, as_completed
from feature_func import all_feature_funcs, all_features
        
# separated from class to allow for parallel processing
def _compute_day(
        l2: pl.DataFrame,
        l1: pl.DataFrame,
        l2_col_mapping: dict, 
        l1_col_mapping: dict, 
        ob_handler: LocalOrderBook, 
        trade_handler: TradesHandler, 
        dest: str,
        buffer_size: int = 2**20,
    ) -> None:    
    dest = open(dest, 'a', buffering=buffer_size) 
    # replay loop
    prev_data = None
    for (l2_updates, trades) in zip(l2.iter_rows(named = True), l1.iter_rows(named = True)):
        # assure time is uniform
        timestamp = l2_updates.pop('Timestamp')
        assert timestamp == trades.pop('Timestamp')
        
        # process l2 updates
        if l2_updates['Code'] is not None:
            for row in zip(*l2_updates.values()):
                handle_l2_update(row, l2_col_mapping, ob_handler)
        
        # process trades 
        if trades['Code'] is not None:
            for row in zip(*trades.values()):
                if row[l1_col_mapping['Code']] == 'blank':
                    continue
                handle_trades(row, l1_col_mapping, trade_handler)
        
        # record the features
        data = ob_handler.take_snapshot()
        data += trade_handler.get_ohlcva()
        data += [f(data, prev_data) for f in all_feature_funcs]
        dest.write(f"{str(data)[1:-1]}, {timestamp}\n")
        prev_data = data
        
    dest.close()
            
def handle_trades(row, l1_col_mapping, trades_handler) -> None: # message handler wrapper
    price = row[l1_col_mapping['TradeEvent_LastPrice']]
    qty = row[l1_col_mapping['TradeEvent_LastTradeQuantity']]
    trades_handler.handle_trades(price, qty)
    
def handle_l2_update(row, l2_col_mapping, ob_handler) -> None: # message handler wrapper
    # 1.4.4.8   OverlapRefresh
    if row[l2_col_mapping['OverlapRefresh_BidChangeIndicator']] is not None or\
       row[l2_col_mapping['OverlapRefresh_AskChangeIndicator']] is not None:
        handle_OverlapRefresh(row, ob_handler, l2_col_mapping)
    # 1.4.2     DeltaRefresh
    elif row[l2_col_mapping['DeltaRefresh_DeltaAction']] is not None:
        handle_DeltaRefresh(row, ob_handler, l2_col_mapping)
    # 1.4.4.9   MBLMaxVisibleDepth
    elif row[l2_col_mapping['MaxVisibleDepth_MaxVisibleDepth']] is not None:
        handle_MBLMaxVisibleDepth(row, ob_handler, l2_col_mapping)

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
        if bid_is_full:
            ob.BidClearFromLevel(0)
        bid_limits = json.loads(f"[{bid_limits}]".replace('][', '],['))
        for i in range(len(bid_limits)):
            ob.BidOverwriteLevel(bid_limits[i][0], bid_limits[i][1], bid_start_level+i)
    if ask_limits is not None: # load ask limits (snapshot)
        if ob.ask_prices[0] is None:
            ask_start_level = 0
        if ask_is_full:
            ob.AskClearFromLevel(0)
        ask_limits = json.loads(f"[{ask_limits}]".replace('][', '],['))
        for i in range(len(ask_limits)):
            ob.AskOverwriteLevel(ask_limits[i][0], ask_limits[i][1], ask_start_level+i)
                
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
            max_workers: int = 2,
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
        max_workers:    int, number of processes to use for parallel processing
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
        self.blank_update_template = {k:[] for k in L2_SCHEMA.keys()}
        self.blank_trade_template = {k:[] for k in L1_SCHEMA.keys()}
        self.max_workers = max_workers

    def close(self):
        for dest in self.dest_file_streams.values():
            dest.close()

    def compute_day(self):
        """
        computes one day worth of features and write to destination
        """
        self._read_next_date()
        with ProcessPoolExecutor(max_workers=self.max_workers) as pool:
            rs = []
            for code in self.universe:
                rs += [pool.submit(
                    _compute_day,
                    self.curr_data['l2'][code],
                    self.curr_data['trades'][code],
                    self.l2_col_mapping,
                    self.l1_col_mapping,
                    self.ob_container[code],
                    self.trade_handler_container[code],
                    self.dest_file_stream,
                    self.buffer_size,
                )]

        outputs = {}
        # catch exceptions & print progress
        for i, future in enumerate(as_completed(rs)):
            try:
                outputs[self.universe[i]], timestamp = future.result()
                print(f"Finished {self.universe[i] + ' ' + self.date}")
            except Exception as exc:
                print(exc)

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

        print(f"finished loading data for {self.date}")

        if len(self.ob_container) == 0:
            self.init_params()
        l2_schema = copy.deepcopy(L2_SCHEMA)
        l2_schema['Timestamp'] = pl.Datetime
        l1_schema = copy.deepcopy(L1_SCHEMA)
        l1_schema['Timestamp'] = pl.Datetime
        self.time = datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.timedelta(hours=2)
        blank_update = copy.deepcopy(self.blank_update_template)
        blank_trade = copy.deepcopy(self.blank_trade_template)
        self._insert_to_end(blank_update, blank_trade, l2_schema, l1_schema, date)
        self._insert_to_start(blank_update, blank_trade, l2_schema, l1_schema)

        print(f"finished blank l1/l2 message insertion for {self.date}")

        # sanity check
        msg = f"l2 and l1 dataframes have different min timestamps: {self.curr_data['l2']['Timestamp'].min()},\
                {self.curr_data['trades']['Timestamp'].min()}"
        assert self.curr_data['l2']['Timestamp'].min() == self.curr_data['trades']['Timestamp'].min() == self.time, msg
        msg = f"l2 and l1 dataframes have different max timestamps: {self.curr_data['l2']['Timestamp'].max()},\
                {self.curr_data['trades']['Timestamp'].max()}"
        assert self.curr_data['l2']['Timestamp'].max() == self.curr_data['trades']['Timestamp'].max()\
                == self.time + datetime.timedelta(hours=24) - self.freq / 2, msg


        # partition all data by instrument for parallel processing
        self.curr_data['l2'] = self.curr_data['l2'].partition_by(
            by='Code',
            maintain_order=True,
            include_key=True,
            as_dict=True
        )
        self.curr_data['trades'] = self.curr_data['trades'].partition_by(
            by='Code',
            maintain_order=True,
            include_key=True,
            as_dict=True
        )

        print(f"finished partitioning by instrument code for {self.date}")

        # aggregating and upsampling to make the data time uniform
        # each row in the dataframe is a collection of messages that happened in this interval
        for code in self.universe:
            self.curr_data['l2'][code] = self.curr_data['l2'][code].group_by_dynamic(
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
                    pl.col('_upper_boundary').alias('Timestamp'),
                ).upsample(
                    time_column="Timestamp",
                    every=self.freq
                )
            self.curr_data['trades'][code] = self.curr_data['trades'][code].group_by_dynamic(
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
            self.l2_col_mapping = {col: i for i, col in enumerate(self.curr_data['l2'][self.universe[0]].columns[1:])}
            self.l1_col_mapping = {col: i for i, col in enumerate(self.curr_data['trades'][self.universe[0]].columns[1:])}
            # the first column is the timestamp, which is not in the schema

        shapes = [self.curr_data['l2'][code].shape[0] for code in self.universe]
        assert len(set(shapes)) == 1, f"l2 dataframes have different shapes: {shapes}"
        shapes = [self.curr_data['trades'][code].shape[0] for code in self.universe]
        assert len(set(shapes)) == 1, f"l1 dataframes have different shapes: {shapes}"

        print(f"finished preprocessing for date {self.date}")

    def _insert_to_end(self, blank_update, blank_trade, l2_schema, l1_schema, date):
        # insert a blank message to end of trades/l2 data
        # to ensure uniform sampling
        self.curr_data['l2'] = self.curr_data['l2'].set_sorted(
            'Timestamp',
            descending=False
        ).filter(
            pl.col('Timestamp') < self.time + datetime.timedelta(days=1)
        )
        eod_time = [self.time + datetime.timedelta(days=1) - self.freq/2]
        blank_trade['Timestamp'] = eod_time * len(self.universe)
        blank_update['Timestamp'] = eod_time * len(self.universe)
        self.curr_data['trades'] = pl.concat([
                self.curr_data['trades'],
                pl.DataFrame(blank_trade, schema_overrides=l1_schema),
            ]).set_sorted(
                column='Timestamp',
                descending=False
            )
        self.curr_data['l2'] = pl.concat([
                self.curr_data['l2'],
                pl.DataFrame(blank_update, schema_overrides=l2_schema),
            ]).set_sorted(
                column='Timestamp',
                descending=False
            )

    def _insert_to_start(self, blank_update, blank_trade, l2_schema, l1_schema):
        # insert a blank message to start of trades/l2 data
        self.curr_data['l2'] = self.curr_data['l2'].set_sorted(
            column='Timestamp',
            descending=False
        ).filter(
            pl.col('Timestamp') >= self.time
        )
        blank_update['Timestamp'] = [self.time] * len(self.universe)
        blank_trade['Timestamp'] = [self.time] * len(self.universe)
        self.curr_data['l2'] = pl.concat([
            pl.DataFrame(blank_update, schema_overrides=l2_schema),
            self.curr_data['l2'],
        ]).set_sorted(
            column='Timestamp',
            descending=False
        )
        self.curr_data['trades'] = pl.concat([
            pl.DataFrame(blank_trade, schema_overrides=l1_schema),
            self.curr_data['trades'],
        ]).set_sorted(
            column='Timestamp',
            descending=False
        )

    def __repr__(self) -> str:
        return f"Replayer({self.dir}, {self.eid}, {self.freq})"

    def init_params(self):
        if self.universe == []:
            self.universe = self.curr_data['l2']['Code'].unique().to_list()
            self.universe = [code for code in self.universe if code != 'blank']
        self.blank_update_template['Code'] = copy.deepcopy(self.universe)
        self.blank_trade_template['Code'] = copy.deepcopy(self.universe)
        for key in self.blank_trade_template.keys():
            if key != 'Code':
                self.blank_trade_template[key] = [None] * len(self.universe)
        for key in self.blank_update_template.keys():
            if key != 'Code':
                self.blank_update_template[key] = [None] * len(self.universe)
        self.ob_container = {code: LocalOrderBook(code) for code in self.universe}
        self.trade_handler_container = {code: TradesHandler(code, self.freq) for code in self.universe}
        self.time = datetime.datetime.strptime(self.date, "%Y-%m-%d") - datetime.timedelta(hours=2)
        self.dest_file_streams = {
            code: os.path.join(self.dest, f"{code}.csv")
            for code in self.universe
        }
        print(self.dest_file_streams.keys())
        features = [f'bid_price_{i}' for i in range(10)] + [f'bid_qty_{i}' for i in range(10)] +\
                   [f'ask_price_{i}' for i in range(10)] + [f'ask_qty_{i}' for i in range(10)] +\
                   ['open', 'high', 'low', 'close', 'volume', 'amount'] +\
                   all_features + ['timestamp']
        features = ', '.join(features)
        for dest in self.dest_file_streams.values():
            with open(dest, 'w+') as dest:
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
