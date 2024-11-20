Python based orderbook replayer for https://hod.iress.com/help/dataguide/#


Usage:  python main.py <source> <destination> <days_to_replay>

    Params:
    -------
    path:           str, absolute path to the directory containing the data
    eid:            str, exchange id
    dest:           str, absolute path to the directory to store the output
    frequency:      timedelta, frequency of data replay
    universe:       list, string codes of all instruments, if None, will be inferred from data of start date
    buffer_size:    int, buffer size of the output file streams
    start:          str, start date of the data to be replayed, format: YYYY-MM-DD