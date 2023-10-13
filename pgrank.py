"""
It tests pregel.py by computing the PageRank for the same graph in a
different, more conventional way, and showing that the two outputs are
near-identical. 
"""

from src.pregel import Vertex, Pregel

# The next two imports are only needed for the test.  
from numpy import mat, eye, zeros, ones, linalg
import random
import time

num_workers = 6
num_vertices = 100

def main():
    vertices = [PageRankVertex(j,1.0/num_vertices,[]) 
                for j in range(num_vertices)]
    create_edges(vertices)
    pr_test = pagerank_test(vertices)
    # print(f"Test computation of pagerank:\n{pr_test}")
    pr_pregel = pagerank_pregel(vertices)
    # print(f"Pregel computation of pagerank:\n{pr_pregel}")
    diff = pr_pregel-pr_test
    # print(f"Difference between the two pagerank vectors:\n{diff}")
    print(f"The norm of the difference is: {linalg.norm(diff)}")

def create_edges(vertices):
    """Generates 4 randomly chosen outgoing edges from each vertex in
    vertices."""
    for vertex in vertices:
        vertex.edges = random.sample(vertices,4)

def pagerank_test(vertices):
    """Computes the pagerank vector associated to vertices, using a
    standard matrix-theoretic approach to computing pagerank.  This is
    used as a basis for comparison."""
    a = time.time()
    I = mat(eye(num_vertices))
    G = zeros((num_vertices,num_vertices))
    for vertex in vertices:
        num_out_vertices = len(vertex.edges)
        for out_vertex in vertex.edges:
            G[out_vertex.id,vertex.id] = 1.0/num_out_vertices
    P = (1.0/num_vertices)*mat(ones((num_vertices,1)))
    b = time.time()
    elapsed_time = (b - a) * 1000
    print(f'Time taken for NORMAL: {elapsed_time:.2f} milliseconds')
    return 0.15*((I-0.85*G).I)*P

def pagerank_pregel(vertices):
    """Computes the pagerank vector associated to vertices, using
    Pregel."""
    a = time.time()
    p = Pregel(vertices,num_workers)
    p.run()
    b = time.time()
    elapsed_time = (b - a) * 1000
    print(f'Time taken for PREGEL: {elapsed_time:.2f} milliseconds')
    return mat([vertex.value for vertex in p.graph]).transpose()

class PageRankVertex(Vertex):

    def update(self):
        # This routine has a bug when there are pages with no outgoing
        # links (never the case for our tests).  This problem can be
        # solved by introducing Aggregators into the Pregel framework,
        # but as an initial demonstration this works fine.
        # print("IS IT WORKING")
        if self.superstepNum < 50:
            self.value = 0.15 / num_vertices + 0.85*sum(
                [pagerank for (vertex,pagerank) in self.incomingMessages])
            outgoing_pagerank = self.value / len(self.edges)
            self.outgoingMessages = [(vertex,outgoing_pagerank) 
                                      for vertex in self.edges]
        else:
            self.isActive = False

if __name__ == "__main__":
    main()
