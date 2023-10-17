import os
import signal
from multiprocessing import Process
from abc import abstractmethod, ABC

class Worker(ABC):
    def __init__(self, idx, tot, lock):
        self.id = idx
        self.numWorkers = tot
        self.lock = lock
        self.process : Process
        self.pid = -1
    
    def create_and_run(self):
        self.process = Process(target=self.run, args=())
        self.process.start()
        self.pid = self.process.pid
    
    def workerHash(self, vertex):
        """ Returns the id of the worker that vertex is assigned to """
        return int(hash(vertex.id) % self.numWorkers)
    
    @abstractmethod
    def run(self):
        raise NotImplementedError
    
    def wait(self):
        self.process.join()
