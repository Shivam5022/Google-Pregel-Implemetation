import collections
import multiprocessing
import pickle
from redis.client import Redis
from src.vertex import Vertex
from src.threads import WcWorker

class Pregel():
    """
        This is the main pregel class, which our user program will use.
        It will take the graph (in form of all vertices and associated data), total number of workers as input.

        graph: it is basically list of vertices. (each vertex will contain its associated data)
        numWorkers: total number of worker processes. currently all of them will run in same machine in diff cores
    """
    def __init__(self, graph, numWorkers, flag):
        self.graph = graph
        self.numWorkers = numWorkers
        self.parallel = flag
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
            partitions[self.workerHash(vertex)].append(vertex)
        return partitions
    
    def graphActive(self):
        """
            This function returns TRUE if any of the vertex in graph is active
        """
        for id in range(self.numWorkers):
            partition = pickle.loads(self.rds.get(id))
            for vertex in partition:
                if vertex.isActive == True:
                    return True
        
        return False
    
    def run(self):
        """
            Running the Pregel framework on input graph:
            1. Create the partitions
            2. While the graph is active, carry out the superstep and then pass messages.
                (Here the worker dies after carrying out the superstep)
        """
        self.partitions = self.partition()
        for key, value in self.partitions.items():
            value_str = pickle.dumps(value)
            self.rds.set(key, value_str)

        f = 0
        while self.graphActive():
            print("Running Superstep:",f)
            self.superstep()
            print("Communication started")
            self.messagePassing()
            print("Communication ended")
            f += 1
        
        self.graph = [None] * len(self.graph)
        for id in range(self.numWorkers):
            partition = pickle.loads(self.rds.get(id))
            for vertex in partition:
                self.graph[vertex.id] = vertex
    
    def superstep(self):
        """
            Here the new workers are spawned in each superstep!
            Try to make the workers persistent, and to use a locking mechanism to
            synchronize!!
        """
        if self.parallel:
            A = []
            idx = 0
            for partition in self.partitions.values():
                A.append(WcWorker(idx = idx))
                idx += 1
            
            assert(idx == self.numWorkers)

            for workers in A:
                workers.create_and_run()

            for workers in A:
                workers.wait()
        
        else:
            print("SERIAL")
            for partition in self.partitions.values():
                for vertex in partition:
                    if vertex.isActive:
                        vertex.update()
        
    
    def messagePassing(self):
        for id in range(self.numWorkers):
            partition = pickle.loads(self.rds.get(id))
            for vertex in partition:
                vertex.superstepNum += 1
                vertex.incomingMessages = []
            self.rds.set(id, pickle.dumps(partition))


        for id in range(self.numWorkers):
            partition = pickle.loads(self.rds.get(id))
            for vertex in partition:
                for (destination, message) in vertex.outgoingMessages:
                    temp = self.workerHash(destination)
                    p = pickle.loads(self.rds.get(temp))
                    for v in p:  # here i am iterating on complete partition. make this O(1)
                        assert(type(v) == type(destination))
                        if v.id == destination.id:
                            v.incomingMessages.append((vertex, message))
                    self.rds.set(temp, pickle.dumps(p))

