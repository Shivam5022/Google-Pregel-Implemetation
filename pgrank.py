"""
It tests pregel.py by computing the PageRank for the same graph in a
different, more conventional way, and showing that the two outputs are
near-identical. 
"""

from src.pregel import Vertex, Pregel

from numpy import mat, eye, zeros, ones, linalg
import random
import time

num_workers = 6

num_vertices = 500

def main():
    vertices = [PageRankVertex(j,1.0/num_vertices,[]) 
                for j in range(num_vertices)]
    create_edges(vertices)
    pr_test = pagerank_test(vertices)
    pr_pregel = pagerank_pregel(vertices)
    # print(f"Test computation of pagerank:\n{pr_test}")
    # print(f"Pregel computation of pagerank:\n{pr_pregel}")
    # print(f"Difference between the two pagerank vectors:\n{diff}")
    diff = pr_pregel-pr_test
    print(f"The norm of the difference is: {linalg.norm(diff)}")

def create_edges(vertices):
    """Generates 4 randomly chosen outgoing edges from each vertex in
    vertices."""
    i = 0
    op = []
    for i in range(num_vertices):
        op.append(i)
    for vertex in vertices:
        vertex.edges = random.sample(op, 3)

def pagerank_test(vertices):
    """Computes the pagerank vector associated to vertices, using a
    standard matrix-theoretic approach to computing pagerank.  This is
    used as a basis for comparison."""
    I = mat(eye(num_vertices))
    G = zeros((num_vertices,num_vertices))
    for vertex in vertices:
        num_out_vertices = len(vertex.edges)
        for out_vertex in vertex.edges:
            G[out_vertex,vertex.id] = 1.0/num_out_vertices
    P = (1.0/num_vertices)*mat(ones((num_vertices,1)))
    print("Normal Computation is finished !")
    return 0.15*((I-0.85*G).I)*P

def pagerank_pregel(vertices):
    """Computes the pagerank vector associated to vertices, using
    Pregel."""
    p = Pregel(vertices, num_workers)
    output = p.run()
    return mat([vertex.value for vertex in output]).transpose()

class PageRankVertex(Vertex):

    def update(self):
        if self.superstepNum < 20:
            self.value = 0.15 / num_vertices + 0.85*sum(
                [pagerank for (z,pagerank) in self.incomingMessages])
            outgoing_pagerank = self.value / len(self.edges)
            self.outgoingMessages = [(vertexID, outgoing_pagerank) 
                                      for vertexID in self.edges]
        else:
            self.isActive = False

if __name__ == "__main__":
    main()