import os
import signal
from multiprocessing import Process, Manager
from abc import abstractmethod, ABC

class Worker(ABC):
    def __init__(self, idx, tot, chunk):
        self.id = idx
        self.numWorkers = tot
        self.partition = chunk
        self.process : Process
        self.current = 0
        self.pid = -1
    
    def create_and_run(self):
        self.process = Process(target=self.run, args=())
        self.process.start()
        self.pid = self.process.pid
    
    @abstractmethod
    def run(self):
        raise NotImplementedError
    
    def wait(self):
        self.process.join()
    
    def is_alive(self):
        # Check if the worker process is alive
        return self.process.is_alive()
    
    def kill(self):
        self.process.terminate()
