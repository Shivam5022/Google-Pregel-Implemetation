import pickle
import time
import sys
import os
from redis.client import Redis
from src.base import Worker
import threading

def save_checkpoint(incoming_messages, vertex_values, vertex_active, superstep, id):
    checkpoint_data = {
        "values": vertex_values,
        # "current_superstep": superstep,
        "incoming_messages": incoming_messages,
        "actives": vertex_active
    }
    folder_name = 'checkpoint'

    with open(f"{folder_name}/checkpoint_worker_{id}_superstep_{superstep}.pkl", "wb") as checkpoint_file:
        pickle.dump(checkpoint_data, checkpoint_file)


def heartbeat(rds, id, exit_flag):
    while not exit_flag.is_set():
        time.sleep(1)
        rds.set(f'alive_{id}',1)
    print(f"Worker with id {id} is dead")

class WcWorker(Worker):
    def run(self):
        print(f'Starting run function of id {self.id}')
        key = self.id   # this is the ID of this worker
        rds = Redis(host='localhost', port=6379, db=0, decode_responses=False)  # make the connection
        exit_flag = threading.Event()

        thread = threading.Thread(target=heartbeat, args=(rds,self.id,exit_flag,))
        thread.start()
        # print(f"Created heartbeat thread of {self.id}")
        while True:    # Running Supersteps constantly
            # print(f"id {key} started running supersteps")
            """
            Changes if a worker dies: (Fault Tolerence):
                Checkpoint at frequency what paper suggests
            1.  let file 'f' contains the latest checkpointed partition (let superstep S) of this worker.
                it will basically store all vertices and its associated data. 
                The vertices of this file must be assigned to other alive workers.
            2.  all workers will have to start from S again.
            3.  what all will be updated for each worker:
                a. self.numWorkers (will be reduced by dead workers count)
                b. self.partition (load its partition from file + some new vertices of dead worker's partition)
                c. self.current (S' will change to S)
                d. (f"msg:{vertex.id}") list in redis for each vertex in the partition. (store these msgs too in file)
            """

            done = False

            # LOGIC OF BREAKING HERE (CHECK IF ANY VERTEX IS ACTIVE!)
            if int(rds.get("active")) == 0:
                done = True

            if done:
                print(f"Worker {key} has finished the task")
                for vertex in self.partition:
                    # Loading output partition in Redis, which pregel client will retrive
                    rds.hset("vertices", vertex.id, pickle.dumps(vertex))
                break
            
            # Barrier_1 here
            a = "b1_" + str(self.current)
            rds.incr(a)
            while int(rds.get(a)) != self.numWorkers:
                wait = 1
            rds.set("active", 0)  # After sync each worker is setting it 0.
            
            incoming_messages = {}
            vertex_values = {}
            vertex_active = {}
            # Now perfom the compute() function on its partition
            for vertex in self.partition:
                while True: # Delivering incoming messages to this vertex
                    z = rds.rpop(f"msg:{vertex.id}")
                    if z == None:
                        break
                    z = pickle.loads(z)
                    vertex.incomingMessages.append(z)
                incoming_messages[vertex.id] = vertex.incomingMessages
                vertex_values[vertex.id] = vertex.value
                vertex_active[vertex.id] = vertex.isActive  
                if vertex.isActive:
                    print(f"Process {key} is working on vertex ID {vertex.id} in superstep {vertex.superstepNum}")
                    # print(f"Process {key} is working on vertex ID {vertex.id}, {vertex.value}, {vertex.incomingMessages} in superstep {vertex.superstepNum}")
                    vertex.update() # calling update function on this vertex
                    vertex.superstepNum += 1
                    vertex.incomingMessages = [] 
                    self.current = vertex.superstepNum
            
            if(self.current%2==0): # checkpointing
                save_checkpoint(incoming_messages, vertex_values, vertex_active, self.current-1, self.id)

            if(self.id == 1 and self.current==7):
                print(f"Killing worker with id {self.id}")
                exit_flag.set()
                sys.exit()

            # Barrier_2 here
            b = "b2_" + str(self.current)
            rds.incr(b)
            while int(rds.get(b)) != self.numWorkers:
                wait = 1
            
            """
                Now do message passing:
                1. iterate on all the vertices of worker's partition and put
                   the outgoing messages in the destination. Use Redis here!
                   rds.rpush()
                2. Later in next superstep, each vertex will mpull the messages into
                   its incomingMessages from redis. rds.rpop() [Done this thing above.]
            """
            for vertex in self.partition:
                if vertex.isActive:
                    rds.incr("active")  # INCREMENT it if the vertex is active
                for (destination, message) in vertex.outgoingMessages:
                    dID = destination # destination (ID of the vertex)
                    z = (vertex.id, message)
                    rds.rpush(f"msg:{dID}", pickle.dumps(z))  # sending message to destination

            # Barrier_3 here
            c = "b3_" + str(self.current)
            rds.incr(c)
            while int(rds.get(c)) != self.numWorkers:
                wait = 1
            continue
        
        exit_flag.set()
        rds.set(f'done_{id}',1)