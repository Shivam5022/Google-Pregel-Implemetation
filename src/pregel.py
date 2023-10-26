import collections
import pickle
import socket
from multiprocessing import Process, Lock
from redis.client import Redis
from src.vertex import Vertex
from src.worker import WcWorker

class Pregel():
    """
        This is the main pregel class, which our user program will use.
        It will take the graph (in form of all vertices and associated data), total number of workers as input.

        graph: it is basically list of vertices. (each vertex will contain its associated data)
        numWorkers: total number of worker processes. currently all of them will run in same machine in diff cores
    """
    def __init__(self, graph, numWorkers):
        self.graph = graph
        self.numVertices = len(graph)
        self.numWorkers = numWorkers
        self.rds = Redis(host='localhost', port=6379, db=0, decode_responses=False)
    
    def workerHash(self, vertex):
        """ Returns the id of the worker that vertex is assigned to """
        return int(hash(vertex.id) % self.numWorkers)
    
    def partition(self):
        """
            PAPER: The Pregel library divides a graph into partitions, each
            consisting of a set of vertices and all of those vertices’ outgoing edges. 
            Assignment of a vertex to a partition depends
            solely on the vertex ID, which implies it is possible to know
            which partition a given vertex belongs to even if the vertex is
            owned by a different machine, or even if the vertex does not
            yet exist. 
            The default partitioning function is just hash(ID)
            mod N, where N is the number of partitions, but users can
            replace it.
        """
        partitions = collections.defaultdict(list)
        for vertex in self.graph:
            workerID = self.workerHash(vertex)  # This worker will contain this vertex
            partitions[workerID].append(vertex)
        
        # Retrieving all vertex IDs owned by a specific worker (partition)
        # partitionIDs = self.rds.smembers(f"workerPartition:{workerID}")

        return partitions # returning the partition
    
    def run(self):
        """
            Running the Pregel framework on input graph:
            1. Create the partitions
            2. While the graph is active, carry out the superstep and then pass messages.
                (Here the worker dies after carrying out the superstep)
        """
        
        self.rds.flushall()  # clearing the redis database
        partitions = self.partition() # creating partitions and assigning data

        for i in range(0, 1000):  # this is for syncronization in supersteps. (Handles 1000 supersteps)
            a = "b1_" + str(i)
            b = "b2_" + str(i)
            c = "b3_" + str(i)
            self.rds.set(a, 0)
            self.rds.set(b, 0)
            self.rds.set(c, 0)
        
        self.rds.set("active", self.numVertices)

        Pool = []
        for idx in range(self.numWorkers):
            worker = (WcWorker(idx = idx, tot = self.numWorkers, chunk = partitions[idx]))
            Pool.append(worker)
            worker.create_and_run()

        for workers in Pool:
            workers.wait()
        
        # Copying the final graph from redis into output graph

        output = [None] * self.numVertices
        for id in range(self.numVertices):
            output[id] = pickle.loads(self.rds.hget("vertices", id))

        return output
    

