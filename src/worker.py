import pickle
import time
import os
import copy
import socket
from redis.client import Redis
from src.base import Worker

class WcWorker(Worker):
    def run(self):
        key = self.id      # this is the ID of this worker
        rds = Redis(host='localhost', port=6379, db=0, decode_responses=False)  # make the connection

        while True:    # Running Supersteps constantly
            current = 0
            done = True

            # This ending of supersteps part is incomplete!
            # ADD LOGIC OF BREAKING HERE (CHECK IF ANY VERTEX IS ACTIVE!)
            # Can use Redis here for synchronnization!

            for vertex in self.partition:
                current = vertex.superstepNum
                if vertex.superstepNum <= 20:
                    done = False

            if done:
                print(f"Worker {key} has finished the task")
                for vertex in self.partition:
                    # Loading output partition in Redis, which pregel client will retrive
                    rds.hset("vertices", vertex.id, pickle.dumps(vertex))
                break
            
            # Now perfom the cmopute() function on its partition
            for vertex in self.partition:
                while True: # Delivering incoming messages to this vertex
                    z = rds.rpop(f"xxx:{vertex.id}")
                    if z == None:
                        break
                    z = pickle.loads(z)
                    vertex.incomingMessages.append(z)
                if vertex.isActive:
                    print(f"Process {key} is working on vertex ID {vertex.id} in superstep {vertex.superstepNum}")
                    vertex.update() # calling update function on this vertex
                    vertex.superstepNum += 1
                    vertex.incomingMessages = [] 
            
            # Barrier_1 here
            current = int(current)
            a = "b1_" + str(current)
            rds.incr(a)
            while int(rds.get(a)) != self.numWorkers:
                wait = 1
            
            """
                Now do message passing:
                1. iterate on all the vertices of worker's partition and put
                   the outgoing messages in the destination. Use Redis here!
                   rds.rpush()
                2. Later in next superstep, each vertex will pull the messages into
                   its incomingMessages from redis. rds.rpop() [Done this thing above.]
            """
            
            for vertex in self.partition:
                for (destination, message) in vertex.outgoingMessages:
                    dID = destination # destination (ID of the vertex)
                    z = (vertex.id, message)
                    rds.rpush(f"xxx:{dID}", pickle.dumps(z))  # sending message to destination
            
            # Barrier_2 here
            b = "b2_" + str(current)
            rds.incr(b)
            while int(rds.get(b)) != self.numWorkers:
                wait = 1
            
            continue
            assert(False)
