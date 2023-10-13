import collections
from src.vertex import Vertex
from src.worker import Worker

class Pregel():
    """
        This is the main pregel class, which our user program will use.
        It will take the graph (in form of all vertices and associated data), total number of workers as input.

        graph: it is basically list of vertices. (each vertex will contain its associated data)
        numWorkers: total number of worker processes. currently all of them will run in same machine in diff cores
    """
    def __init__(self, graph, numWorkers):
        self.graph = graph
        self.numWorkers = numWorkers
    
    def workerHash(self, vertex):
        """ Returns the id of the worker that vertex is assigned to """
        return hash(vertex.id) % self.numWorkers
    
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
        for vertex in self.graph:
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
        while self.graphActive():
            self.superstep()
            self.messagePassing()
    
    def superstep(self):
        """
            Here the new workers are spawned in each superstep!
            Try to make the workers persistent, and to use a locking mechanism to
            synchronize!!
        """
        workers = []
        for partition in self.partitions.values():
            worker = Worker(partition)
            workers.append(worker)
            worker.start()
        
        for worker in workers:
            worker.join()
    
    def messagePassing(self):
        for vertex in self.graph:
            vertex.superstepNum += 1
            vertex.incomingMessages = []
        
        for vertex in self.graph:
            for (destination, message) in vertex.outgoingMessages:
                # pass
                # How to pass message to the destination vertex ?
                # can we do:
                destination.incomingMessages.append((vertex, message))
                # destination.isActive = True  #paper says to mark it active, but gives inf loop here?