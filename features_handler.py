import numpy as np

class FeaturesHandler:

    def __init__(
        self,
        features: list,
        ob_depth: int = 10
    ):
        self.features = features
        self.look_back = max([f.lookback for f in features])
        self.cached_history = np.zeros((10, 10))
        self.ob_depth = ob_depth
        self.names = [f.name for f in features]
        self.mask = np.array([f.write for f in features])

    def compute(
        self,
        all_input: dict
    ):
        result = dict()
        for symbol in all_input.keys():
            current_input = np.array(all_input[symbol], dtype = np.float32)
            ob_end_idx = self.ob_depth * 4
            snapshot = current_input[:ob_end_idx]
            ohlcva = current_input[ob_end_idx:ob_end_idx+6]
            computed_features = []
            for i, feature in enumerate(self.features):
                lookback = self.cached_history[:, i]
                computed_features += [feature.compute(snapshot=snapshot, ohlcva=ohlcva, lookback=lookback)]
            result[symbol] = np.array(computed_features)[self.mask].tolist()
        return result
