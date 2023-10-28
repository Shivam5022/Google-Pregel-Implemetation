from abc import abstractmethod, ABC

class Aggregator(ABC):
    def __init__(self, vertices, aggr):
        self.vertices = vertices
        self.aggr = aggr 

    @abstractmethod
    def aggregate(self, value):
        raise NotImplementedError 

    def call(self):
        for i in self.vertices:
            self.aggr = self.aggregate(i)
        