import pickle
from redis.client import Redis
from src.worker import Worker

class WcWorker(Worker):
    def run(self):
        key = self.id
        rds = Redis(host='localhost', port=6379, db=0, decode_responses=False)  # make the connection

        



                    
        partition = pickle.loads(rds.get(key))
        for vertex in partition:
            if vertex.isActive:
                print(f"Process {self.id} is working on {vertex.id} in superstep {vertex.superstepNum}")
                vertex.update()
        
        rds.set(key, pickle.dumps(partition))