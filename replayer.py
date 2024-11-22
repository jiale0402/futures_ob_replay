import os
import datetime
import copy
import pgzip
import polars as pl
from concurrent.futures import ProcessPoolExecutor, as_completed
from data_schema import L2_SCHEMA, L1_SCHEMA
from orderbook import LocalOrderBook
from trades import TradesHandler
from feature_func import all_features
from handlers import compute_day


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
        features:       list, feature classes to be computed along with snapshots and ohlcva
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
        self.ob_container = dict() # {instrument: {layerID: LocalOrderBook}}
        self.dest = dest
        self.buffer_size = buffer_size
        os.makedirs(self.dest, exist_ok=True)
        self.blank_update_template = {k:[] for k in L2_SCHEMA.keys()}
        self.blank_trade_template = {k:[] for k in L1_SCHEMA.keys()}
        self.max_workers = max_workers
        self.carry_over = []

    def compute_day(self):
        """
        computes one day worth of features and write to destination
        """
        self._read_next_date()
        all_carry_over = []

        with ProcessPoolExecutor(max_workers=self.max_workers) as pool:
            rs = []
            for carry_over, code in zip(self.carry_over, self.universe):
                rs += [pool.submit(
                    compute_day,
                    self.curr_data['l2'][code],
                    self.curr_data['trades'][code],
                    self.l2_col_mapping,
                    self.l1_col_mapping,
                    self.ob_container[code],
                    self.trade_handler_container[code],
                    self.dest_file_streams[code],
                    self.buffer_size,
                    carry_over
                )]

        # catch exceptions & print progress
        for i, future in enumerate(as_completed(rs)):
            try:
                carry_over, accuracy = future.result()
                print(f"finished {self.universe[i] + ' ' + self.date} with accuracy {accuracy}")
                allcarry_over += [carry_over]
                print(f"finished {self.universe[i] + ' ' + self.date}")
            except Exception as exc:
                print(f"failed {self.universe[i] + ' ' + self.date}")
                print(exc)

        self.carry_over = all_carry_over


    def _read_next_date(self) -> None:
        # read next date's data
        try:
            self.date = self.dates.pop(0)
        except:
            raise ValueError("No more data to be replayed")
        date, directory, eid = self.date, self.dir, self.eid

        # load l2 data
        self.curr_data.clear()
        self.curr_data['date'] = date
        self.curr_data['l2'] = (
            pl.scan_csv(
                os.path.join(directory, "l2_data", f"{date}_{eid}_L2.csv.gz"),
                schema=L2_SCHEMA,
                low_memory=True,
            )
            .filter(pl.col("Code").is_in(self.universe))
            .collect()
        )

        # load l1 data
        self.curr_data['trades'] = (
            pl.scan_csv(
                os.path.join(directory, "l1_data", f"{date}_{eid}_L1-Trades.csv.gz"),
                schema=L1_SCHEMA,
                low_memory=True,
            )
            .filter(pl.col("Code").is_in(self.universe))
            .collect()
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
                == self.time + datetime.timedelta(hours=24) - self.freq / 100, msg


        # partition all data by instrument for parallel processing
        self.curr_data['l2'] = self.curr_data['l2'].partition_by(
            by='Code',
            maintain_order=True,
            include_key=True,
            as_dict=True
        )
        self.curr_data['l2'] = {
            code[0]: data
            for code, data in self.curr_data["l2"].items()
        }
        self.curr_data['trades'] = self.curr_data['trades'].partition_by(
            by='Code',
            maintain_order=True,
            include_key=True,
            as_dict=True
        )
        self.curr_data["trades"] = {
            code[0]: data
            for code, data in self.curr_data["trades"].items()
        }

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
                    pl.col("LayerId"),
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
                    pl.col("LayerId"),
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
        eod_time = [self.time + datetime.timedelta(days=1) - self.freq/100]
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
        self.carry_over = [None] * len(self.universe)
        self.blank_update_template['Code'] = copy.deepcopy(self.universe)
        self.blank_trade_template['Code'] = copy.deepcopy(self.universe)
        for key in self.blank_trade_template.keys():
            if key != 'Code':
                self.blank_trade_template[key] = [None] * len(self.universe)
        for key in self.blank_update_template.keys():
            if key != 'Code':
                self.blank_update_template[key] = [None] * len(self.universe)
        self.ob_container = {
            code: {
                str(layer): LocalOrderBook(code) 
                for layer in range(6)
            } 
            for code in self.universe
        }
        self.trade_handler_container = {code: TradesHandler(code, self.freq) for code in self.universe}
        self.time = datetime.datetime.strptime(self.date, "%Y-%m-%d") - datetime.timedelta(hours=2)
        self.dest_file_streams = {
            code: os.path.join(self.dest, f"{code}.csv")
            for code in self.universe
        }
        print(f"universe: {list(self.dest_file_streams.keys())}")
        orderbook_cols = [f'bid_price_{i}' for i in range(10)] + [f'bid_qty_{i}' for i in range(10)] +\
                     [f'ask_price_{i}' for i in range(10)] + [f'ask_qty_{i}' for i in range(10)]
        orderbooks = [f"layer_{layer}_{col}" for col in orderbook_cols for layer in range(6)]
        features = orderbooks + ['open', 'high', 'low', 'close', 'volume', 'amount'] +\
                   all_features + ['timestamp']
        features = ','.join(features)
        for dest in self.dest_file_streams.values():
            with open(dest, 'w+') as dest:
                dest.write(f"{features}\n")

    def list_dates(self, data_dir) -> list:
        assert os.path.isdir(data_dir), f"{data_dir} is not a directory"
        dates = set()
        for file in os.listdir(os.path.join(data_dir, "l2_data")):
            if file.endswith(".csv.gz") or file.endswith(".csv.gz"):
                dates.add(file.split('_')[0])
        self.time = datetime.datetime.strptime(min(dates), "%Y-%m-%d")
        dates = sorted(list(dates))
        dates = [d for d in dates if d >= self.start]
        return dates
