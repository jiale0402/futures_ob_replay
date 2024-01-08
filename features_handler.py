import numpy as np

class FeaturesHandler:

    def __init__(
        self,
        features: list,
        ob_depth: int = 10
    ):
        self.features = features
        self.look_back = max([f.look_back for f in features])
        self.cached_history = np.zeros((10, 10))
        self.ob_depth = ob_depth
        self.names = [f.name for f in features]

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
                computed_features += [feature.compute(snapshot, ohlcva, self.cached_history[:, i])]

            result[symbol] = computed_features
        return result
