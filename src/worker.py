import pickle
import time
import os
import copy
from redis.client import Redis
from src.base import Worker


class WcWorker(Worker):
    def run(self):
        key = self.id
        rds = Redis(host='localhost', port=6379, db=0, decode_responses=False)  # make the connection
        while True:
            current = 0
            done = True
            for workerID in range(self.numWorkers):    # Logic to finish all the supersteps
                partitionIDs = rds.smembers(f"workerPartition:{workerID}")
                for vertexID in partitionIDs:
                    vertex = pickle.loads(rds.hget("vertices", int(vertexID)))
                    current = copy.deepcopy(vertex.superstepNum)
                    if vertex.isActive == True:
                        done = False
                        break
            if done:
                print(f"Worker {key} has finished the task")
                break   

            # current variable contains superstep number S

            partitionIDs = rds.smembers(f"workerPartition:{key}") # This set contains vertex IDs of all
                                                                  # vertices of this worker's partition

            for vertexID in partitionIDs:
                vertex = pickle.loads(rds.hget("vertices", int(vertexID)))  # This is how we retrieve vertex from redis
                if vertex.isActive:
                    print(f"Process {self.id} is working on {vertex.id} in superstep {vertex.superstepNum}")
                    vertex.update() # calling update function on this vertex
                rds.hset("vertices", vertexID, pickle.dumps(vertex))  # Setting this vertex again after update

            # Barrier_1 here
            current = int(current)
            a = "b1_" + str(current)
            rds.incr(a)
            while int(rds.get(a)) != self.numWorkers:
                wait = 1

            print(f"COMMUNICATION START FOR {current}")

            # START THE COMMUNICATION
            # COMMUNICTION 1 (not bottleneck)

            ab = time.time()
            for vertexID in partitionIDs:
                vertex = pickle.loads(rds.hget("vertices", int(vertexID)))
                vertex.superstepNum += 1
                vertex.incomingMessages = []
                rds.hset("vertices", vertexID, pickle.dumps(vertex))
            bc = time.time()

            elapsed_time = (bc - ab) * 1000
            print(f'maybe chota {elapsed_time:.2f} milliseconds')

            # Barrier_2 here
            b = "b2_" + str(current)
            rds.incr(b)
            while int(rds.get(b)) != self.numWorkers:
                wait = 1

            # TODO: COMMUNICTION 2 is the fucking bottleneck. it has to be optimised.
            """
            WHAT IS HAPPENING HERE:
                1. we are iterating on all vertices of the current partition assigned
                    to this worker
                2. we are then going though all the outgoing messages of a vertex
                3. for a particular (destination, msg) we are finding which worker has
                    this destination vertex currently. and then applying lock on that
                    worker's partition. then iterating on all vertices of that partition
                    and finding which vertex is the destination and then sending it the message.
                4. finally releasing the lock.

                This whole thing has to be optimised.
                a. after finding the destination worker, don't iterate on all the vertices
                    find the vertex in O(1)
                b. applying lock on whole partition (here: destination worker partion)
                    is slowing down things. other processes wont make progress. need to optimise it.

                Maybe change the structure of how data is stored in redis:
                    Currently it is stored as (worker_id, partition) key-value pair.
                    for making any updates, even to a vertex, whole partition is first called
                    (using p = rds.get(id)) and then it is set again after making changes (p -> p') to vertex of this
                    partition (using rds.set(id, p'))
                Think about this management.
            """
            """
                UPDATE:

                1. Added lock for each vertex, instead of whole partition.
                2. Underlying storage changed. Now each vertex is stored individually
                    in redis hset, instead of whole partition. Although a set for each
                    worker is present which gives IDs of all vertices that are assigned
                    to this particular vertex.
            """
            cd = time.time()

            for vertexID in partitionIDs:
                vertex = pickle.loads(rds.hget("vertices", int(vertexID)))
                for (destination, message) in vertex.outgoingMessages:
                    dID = destination # destination ID 
                    self.lock[dID].acquire()  # acquiring lock on this destination vertex, so that others cant access
                    dVertex = pickle.loads(rds.hget("vertices", int(dID))) # destination vertex
                    dVertex.incomingMessages.append((vertexID, message)) # message appended
                    rds.hset("vertices", dID, pickle.dumps(dVertex)) # vertex updated in redis
                    self.lock[dID].release() # lock released after transferring messages
            
            de = time.time()
            elapsed_time = (de - cd) * 1000
            print(f'maybe medium {elapsed_time:.2f} milliseconds')
            elapsed_time = (de - ab) * 1000
            print(f'badaa {elapsed_time:.2f} milliseconds')


            print(f"COMMUNICATION END FOR {current}")

            # Final Barrier_3 here
            c = "b3_" + str(current)
            rds.incr(c)
            while int(rds.get(c)) != self.numWorkers:
                wait = 1
            
            continue
            assert(False)

