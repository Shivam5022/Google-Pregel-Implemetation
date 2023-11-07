class Vertex():
    """
        This defines the vertices of the graph.
        Each Vertex has:
            1. ID to uniquely identify it
            2. Value stored at this vertex
            3. Outgoing edges from this vertex to rest of the graph
                Paper: The directed edges are associated with their
                source vertices, and each edge consists of a modifiable, user
                defined value and a target vertex identifier. (sort of weighted edge)
            4. Incoming messages from previous superstep (S - 1)
            5. Messages to be sent to next superstep (S + 1)
            6. is_active is True if the vertex is active in current superstep
            7. Superstep currently running

            Vertices also have the compute function (same for all):
                It will be defined in the user program which will extend the vertex as base class
    """
    def __init__(self, id, value, edges) -> None:
        self.id = id
        self.value = value
        self.edges = edges  # changed type to ID of the other vertex
        self.incomingMessages = []
        self.outgoingMessages = []
        self.isActive = True
        self.superstepNum = 0
        self.partitionID = -1
    
    def setValue(self, value):
        self.value = value
    
    def addEdge(self, edge):
        self.edges.append(edge)
    
    def getPartitionID(self):
        return self.partitionID
    # TODO: add rest of the functions as we move!