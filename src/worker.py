import pickle
import time
import os
import copy
from redis.client import Redis
from src.base import Worker

"""
we need 2 barriers. 
1st: just after superstep is completed so that communication can start
2nd: after communication is finshed and we can start next superstep

How to add barriers??
(DONE: this part is handled)
"""

class WcWorker(Worker):
    def run(self):
        key = self.id
        rds = Redis(host='localhost', port=6379, db=0, decode_responses=False)  # make the connection
        while True:
            current = 0
            done = True
            for id in range(self.numWorkers):    # Logic to finish all the supersteps
                partition = pickle.loads(rds.get(id))
                for vertex in partition:
                    current = copy.deepcopy(vertex.superstepNum)
                    if vertex.isActive == True:
                        done = False
                        break
            if done:
                break   
            
            partition = pickle.loads(rds.get(key))
            for vertex in partition:
                if vertex.isActive:
                    print(f"Process {self.id} is working on {vertex.id} in superstep {vertex.superstepNum}")
                    vertex.update()
            rds.set(key, pickle.dumps(partition))

            # put the logic of barrier 1 here
            current = int(current)
            a = "b1_" + str(current)
            rds.incr(a)
            while int(rds.get(a)) != self.numWorkers:
                wait = 1

            print(f"COMMUNICATION START FOR {current}")
            ab = time.time()
            # START THE COMMUNICATION
            # COMMUNICTION 1 (not bottleneck)
            partition = pickle.loads(rds.get(key))
            for vertex in partition:
                vertex.superstepNum += 1
                vertex.incomingMessages = []
            rds.set(key, pickle.dumps(partition))

            bc = time.time()

            elapsed_time = (bc - ab) * 1000
            print(f'maybe chota {elapsed_time:.2f} milliseconds')

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
            cd = time.time()
            partition = pickle.loads(rds.get(key))
            for vertex in partition:
                for (destination, message) in vertex.outgoingMessages:
                    temp = self.workerHash(destination)
                    self.lock[temp].acquire()
                    p = pickle.loads(rds.get(temp))
                    for v in p:  # here i am iterating on complete partition. make this O(1)
                        assert(type(v) == type(destination))
                        if v.id == destination.id:
                            v.incomingMessages.append((vertex, message))
                            break
                    rds.set(temp, pickle.dumps(p))
                    self.lock[temp].release()
            
            de = time.time()
            elapsed_time = (de - cd) * 1000
            print(f'maybe medium {elapsed_time:.2f} milliseconds')
            elapsed_time = (de - ab) * 1000
            print(f'badaa {elapsed_time:.2f} milliseconds')


            print(f"COMMUNICATION END FOR {current}")
            # put the logic of barrier 2 here
            c = "b3_" + str(current)
            rds.incr(c)
            while int(rds.get(c)) != self.numWorkers:
                wait = 1
            
            continue
            assert(False)

