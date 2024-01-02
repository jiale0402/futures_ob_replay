from replayer import Replayer
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
        universe:       list, string codes of all instruments, if None, will be inferred from data
        buffer_size:    int, buffer size of the output file streams
        start:          str, start date of the data to be replayed, format: YYYY-MM-DD
        
    """
    r = Replayer(
        "/mnt/ssd1/future/quanthouse/one-mon/cme", 
        eid="1027", 
        dest="/mnt/ssd1/jctemp/dest", 
        frequency=datetime.timedelta(microseconds=100000), # 0.1s
        start="2018-12-01", 
        universe=['648645308']
    )
    days_to_replay = 2
    for i in range(days_to_replay):
        r.compute_day() # this computes one day worth of data