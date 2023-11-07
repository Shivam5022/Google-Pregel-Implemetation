import collections
import pickle
import time
from multiprocessing import Process, Lock
from redis.client import Redis
from src.vertex import Vertex
from src.worker import WcWorker
import glob
import os

def get_checkpoint_data():

    checkpoint_folder = 'checkpoint'
    vertex_values = {}
    vertex_in_messages = {}
    vertex_active = {}

    checkpoint_files = glob.glob(os.path.join(checkpoint_folder, "checkpoint_worker_*_superstep_*.pkl"))

    last_superstep = -1

    for checkpoint_file in checkpoint_files:
        with open(checkpoint_file, 'rb') as file:
            checkpoint_data = pickle.load(file)
        
        superstep_num = int(checkpoint_file.split("_superstep_")[1].split(".")[0])
        last_superstep = max(last_superstep, superstep_num)
    
    for checkpoint_file in checkpoint_files:
        with open(checkpoint_file, 'rb') as file:
            checkpoint_data = pickle.load(file)
        
        vals = checkpoint_data['values']
        in_msgs = checkpoint_data['incoming_messages']
        actives = checkpoint_data['actives']

        superstep_number = int(checkpoint_file.split("_superstep_")[1].split(".")[0])

        if superstep_number == last_superstep:

            for vertex_id, vertex_value in vals.items():
                vertex_values[vertex_id] = vertex_value
            
            for vertex_id, msg in in_msgs.items():
                vertex_in_messages[vertex_id] = msg
            
            for vertex_id, act in actives.items():
                vertex_active[vertex_id] = act
        
    return last_superstep, vertex_values, vertex_in_messages, vertex_active


class Pregel():
    """
        This is the main pregel class, which our user program will use.
        It will take the graph (in form of all vertices and associated data), total number of workers as input.

        graph: it is basically list of vertices. (each vertex will contain its associated data)
        numWorkers: total number of worker processes. currently all of them will run in same machine in diff cores
    """
    def __init__(self, graph, numWorkers):
        self.graph = graph
        self.numVertices = len(graph)
        self.numWorkers = numWorkers
        self.rds = Redis(host='localhost', port=6379, db=0, decode_responses=False)
    
    def workerHash(self, vertex):
        """ Returns the id of the worker that vertex is assigned to """
        return int(hash(vertex.id) % self.numWorkers)
    
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
            workerID = self.workerHash(vertex)  # This worker will contain this vertex
            partitions[workerID].append(vertex)
            vertex.partitionID = workerID
        # Retrieving all vertex IDs owned by a specific worker (partition)
        # partitionIDs = self.rds.smembers(f"workerPartition:{workerID}")

        return partitions # returning the partition
    
    def run(self):
        """
            Running the Pregel framework on input graph:
            1. Create the partitions
            2. While the graph is active, carry out the superstep and then pass messages.
                (Here the worker dies after carrying out the superstep)
        """

        import shutil
        folder_path = 'checkpoint'
        # Check if the folder exists
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            try:
                shutil.rmtree(folder_path)
                os.makedirs(folder_path)
                print("Folder clearing done!!")
            except Exception as e:
                print(f"Failed to delete folder contents: {e}")
        else:
            print("Folder does not exist or is not a directory.")

        self.rds.flushall()  # clearing the redis database
        partitions = self.partition() # creating partitions and assigning data

        for i in range(0, 1000):  # this is for syncronization in supersteps. (Handles 1000 supersteps)
            a = "b1_" + str(i)
            b = "b2_" + str(i)
            c = "b3_" + str(i)
            self.rds.set(a, 0)
            self.rds.set(b, 0)
            self.rds.set(c, 0)
        
        self.rds.set("active", self.numVertices)

        Pool = []
        for idx in range(self.numWorkers):
            worker = (WcWorker(idx = idx, tot = self.numWorkers, chunk = partitions[idx]))
            Pool.append(worker)
            worker.create_and_run()

        alive_worker_ids = [id for id in range(self.numWorkers)]

        for id in alive_worker_ids:        
            self.rds.set(f'done_{id}',0)

        while True:
            """
                1. Check how many processes have died and have completed
                2. Kill all the alive workers
                3. Read the checkpoint files to get the last checkpoint data
                4. Redistribute the partition to the live workers
                5. Update the state of all the vertices
                6. Restart the alive workers
            """
            completed_workers = 0
            for id in alive_worker_ids:
                try:
                    completed_workers += int(self.rds.get(f'done_{id}').decode('utf-8'))
                    self.rds.set(f'done_{id}',0)
                except Exception as e:
                    print(e)
                    self.rds.set(f'done_{id}',0)
            
            
            if completed_workers == self.numWorkers:
                break
            time.sleep(2)
            
            current_alive_workers = []
            for id in alive_worker_ids:
                try:
                    status = int(self.rds.get(f'alive_{id}').decode('utf-8'))
                    if(status==1):
                        current_alive_workers.append(id)
                    self.rds.set(f'alive_{id}',0) 
                except:
                    self.rds.set(f'alive_{id}',0) 
            
            if(len(current_alive_workers)==0):
                break

            if len(current_alive_workers) != len(alive_worker_ids):
                alive_worker_ids = current_alive_workers
                # killing alive workers
                for id in alive_worker_ids:
                    Pool[id].kill()
                self.numWorkers = len(alive_worker_ids)
                new_partition = self.partition()
                # reassigning partitions
                for i in range(self.numWorkers):        
                    Pool[alive_worker_ids[i]].partition = new_partition[i]

                superstep, vertex_data, vertex_inmsg, vertex_active = get_checkpoint_data()

                self.rds.set("active",0)
                for vertex in self.graph:
                    vertex.value = vertex_data[vertex.id]
                    vertex.incomingMessages = vertex_inmsg[vertex.id]
                    vertex.superstepNum = superstep
                    vertex.isActive = vertex_active[vertex.id]
                    vertex.outgoingMessages = []
                    if(vertex.isActive):
                        self.rds.incr("active")
                    while True: # Delivering incoming messages to this vertex
                        z = self.rds.rpop(f"msg:{vertex.id}")
                        if z == None:
                            break
                
                for i in range(0, 1000):  # this is for syncronization in supersteps. (Handles 1000 supersteps)
                    a = "b1_" + str(i)
                    b = "b2_" + str(i)
                    c = "b3_" + str(i)
                    self.rds.set(a, 0)
                    self.rds.set(b, 0)
                    self.rds.set(c, 0)
                
                for id in alive_worker_ids:
                    Pool[id].numWorkers = len(alive_worker_ids)
                    Pool[id].current = superstep
                    Pool[id].create_and_run()
        
        # Copying the final graph from redis into output graph

        output = [None] * self.numVertices
        for id in range(self.numVertices):
            output[id] = pickle.loads(self.rds.hget("vertices", id))

        return output