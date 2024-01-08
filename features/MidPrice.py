from feature import Feature

class MidPrice(Feature):

    def __init__():
        self.lookback = 0
        self.name = "MidPrice"

    def compute(**kwargs):
        snapshot = kwargs['snapshot']
        # infer the index of bid1 and ask1 from shape of snapshot
        ob_depth = int(snapshot.shape[0] / 4)
        bid1 = snapshot[0]
        ask1 = snapshot[2 * ob_depth]
        return (bid1 + ask1) / 2
