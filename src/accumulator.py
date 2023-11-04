from abc import abstractmethod, ABC

class Aggregator(ABC):
    def __init__(self, vertices, aggr):
        self.vertices = vertices
        self.aggr = aggr 

    # user implements the method
    @abstractmethod
    def aggregate(self, value):
        raise NotImplementedError 

    def call(self):
        for i in self.vertices:
            self.aggr = self.aggregate(i)
        

class MaxAggregator(Aggregator):
    def __init__(self, vertices):
        super().__init__(vertices, -1e18)

    def aggregate(self, value):
        self.aggr = max(value, self.aggr)
        return self.aggr
    
class SumAggregator(Aggregator):
    def __init__(self, vertices, offset):
        super().__init__(vertices, offset)

    def aggregate(self, value):
        self.aggr = value + self.aggr
        return self.aggr