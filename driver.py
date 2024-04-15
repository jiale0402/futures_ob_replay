from replayer import Replayer
from features.MidPrice import MidPrice
import datetime

if __name__ == "__main__":
    """

        Replayer: main thread of the feature generation process

        Params:
        -------
        path:           str, absolute path to the directory containing the data
        eid:            str, exchange id
        dest:           str, absolute path to the directory to store the output
        frequency:      timedelta, frequency of data replay
        universe:       list, string codes of all instruments, if None, will be inferred from data of start date
        buffer_size:    int, buffer size of the output file streams
        start:          str, start date of the data to be replayed, format: YYYY-MM-DD

        Output:
        -------
        One csv file (in the destination directory) for each specified symbol in the universe with the following columns:
        bid_price_0 ... bid_price_9
        bid_qty_0   ... bid_qty_9
        ask_price_0 ... ask_price_9
        ask_qty_0   ... ask_qty_9
        open, high, low, close, volue, amount

        ** any user specified feature **

        timestamp

    """
    midprice = MidPrice(write=True)
    r = Replayer(
        "/home/zhaoran/sample",
        eid="1027", 
        dest="/home/zhaoran/sample/output",
        frequency=datetime.timedelta(seconds=1),
        start="2018-12-29", 
        universe=["648438910"]
    )
    days_to_replay = 1
    for i in range(days_to_replay):
        r.compute_day() # this computes one day worth of data
    r.close()
