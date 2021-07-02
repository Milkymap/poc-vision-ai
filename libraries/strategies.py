import zmq 
import cv2 
import pickle 

from os import path 
from glob import glob 
from collections import Counter 

def create_screen(name, W, H, position=None):
    cv2.namedWindow(name, cv2.WINDOW_NORMAL)
    cv2.resizeWindow(name, W, H)
    if position is not None:
        cv2.moveWindow(name, *position)

class ZMQServer:
    def __init__(self, pusher_port, source_data):
        self.pusher_port = pusher_port 
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PUSH)
        self.data = pickle.load(open(source_data, 'rb'))
        self.nb_items = len(self.data)
        self.cursor = 0 
    
    def start(self):
        self.socket.bind(f'tcp://*:{self.pusher_port}')

    def send(self):
        if not self.empty():
            response = self.data[self.cursor]
            self.cursor += 1
            self.socket.send_pyobj(response) 
        else:
            raise ValueError('No item ...!')

    def empty(self):
        return self.cursor == self.nb_items
    
    def close(self):
        self.socket.close()
        self.ctx.term()


class ZMQWorker:
    def __init__(self, pusher_port):
        self.pusher_port = pusher_port 
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PULL)
    
    def connect(self):
        self.socket.connect(f'tcp://localhost:{self.pusher_port}')
    
    def receive(self):
        data = self.socket.recv_pyobj(flags=zmq.NOBLOCK)
        return data
    
    def close(self):
        self.socket.close()
        self.ctx.term()
    
    def process_data(self, metrics): 
        acc = []
        for key, val in metrics.items():
            cnt = Counter()
            iterable = list(val.values())
            cnt.update( iterable )
            acc.append((key, dict(cnt)))
        return dict(acc) 



        
