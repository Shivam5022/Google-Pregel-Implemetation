import threading

class Worker(threading.Thread):
    """
        Worker class extends the threading.Thread class. 
        This code is meant to be part of a multithreaded program where each thread of the 
        Worker class is responsible for processing a set of vertices.
    """
    def __init__(self, vertices):
        threading.Thread.__init__(self)
        self.vertices = vertices

    def run(self):
        self.superstep()

    def superstep(self):
        for vertex in self.vertices:
            if vertex.isActive:
                vertex.update() # This function would be defined in the user program