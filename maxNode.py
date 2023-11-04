
"""
This testcase will be used to test the aggregator functionality. 
We test our pregel implementation for max node (with aggregator) against a simple BFS approach
"""

from src.pregel import Vertex, Pregel

from numpy import mat, eye, zeros, ones, linalg
import random
import time

num_workers = 3

num_vertices = 5000


class MaxVertex(Vertex):
    def update(self):
        pass

class RandomGraph:
    def __init__(self, num_vertices):
        self.graph = [MaxVertex(j, random.randint(0, 4*num_vertices), []) for j in range(num_vertices)]
        def create_edges(vertices):
            """Generates 4 randomly chosen outgoing edges from each vertex in
            vertices."""
            i = 0
            op = []
            for i in range(num_vertices):
                op.append(i)
            for vertex in vertices:
                vertex.edges = random.sample(op, 3)
        create_edges(self.graph)

    def maxNodeBFS(self):
        """
            We use a BFS based search approach to find the 
            maximum value node 
        """
        queue = []
        vis = [False for j in range(num_vertices)]

        queue.append(0)
        vis[0] = True

        max_val = -1
        max_node = -1
        while len(queue) > 0:
            node = self.graph[queue[0]]
            node_val = node.value
            node_id = node.id

            queue.pop(0)

            if node_val > max_val: 
                max_val = node_val 
                max_node = node_id 
            
            for child in node.edges:
                if not vis[child]:
                    queue.append(child)
                    vis[child] = True

        return max_node
    
    def maxNodePregel(self):
        """
            Here we implement a message passing pregel based approach
            to the problem of finding the maximum value node. 
            In this we make use of aggregators to ease out the implementation
        """
        pass 

    def maxNodeIter(self):
        """
            Just a validator to check the correctness of 
            our BFS and Pregel approach
        """
        max_node = -1
        max_val = -1
        for vertex in self.graph: 
            node_val = vertex.value
            node_id = vertex.id 
            if node_val > max_val:
                max_val = node_val
                max_node = node_id
        return max_node 

def main():
    graph = RandomGraph(num_vertices=num_vertices)
    max_node_bfs = graph.maxNodeBFS()
    max_node_iter = graph.maxNodeIter()

    assert max_node_bfs == max_node_iter 

if __name__ == "__main__":
    main()
