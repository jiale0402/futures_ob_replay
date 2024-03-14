from abc import ABC, abstractmethod

class Feature(ABC):

    """
    this is an abstract template of features
    """

    def init(self, name, look_back_window):
        raise NotImplementedError("Init must be overrided")

    @abstractmethod
    def compute(self):
        raise NotImplementedError("Feature subclass must override compute")
