from abc import abstractmethod, ABC

class Aggregator(ABC):
    def __init__(self, aggr):
        self.aggr = aggr 

    def setOffset(self, offset):
        self.aggr = offset

    # user implements the method
    @abstractmethod
    def aggregate(self, value):
        raise NotImplementedError 

    def call(self, incomingMessages):
        for _, val in incomingMessages:
            self.aggregate(val)
        return self.aggr
        

class MaxAggregator(Aggregator):
    def __init__(self, inital):
        super().__init__(inital)

    def aggregate(self, value):
        self.aggr = max(value, self.aggr)
    
class SumAggregator(Aggregator):
    def __init__(self, offset):
        super().__init__(offset)

    def aggregate(self, value):
        self.aggr = value + self.aggr