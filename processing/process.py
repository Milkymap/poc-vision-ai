import click 

import zmq 
import pickle 
from time import sleep 

from collections import Counter 

import multiprocessing as mp 
from libraries.strategies import ZMQServer, ZMQWorker 

def server_loop(pusher_port, source_data, server_status):
    try:
        server = ZMQServer(pusher_port, source_data)
        server.start()  # brancher le server sur le port 
        server_status.set()
        keep_sending = True 
        while keep_sending and not server.empty():
            server.send()
            sleep(1)  # wait 1 second 
    except KeyboardInterrupt as e:
        pass
    finally:
        server_status.clear()  # turn off the server 
        sleep(5)  # 3 seconds 
        server.close()

def worker_loop(pusher_port, pid, server_status):
    try:
        keep_processing = True 
        print('%03d => wait for the server to be ready' % pid)
        server_status.wait()  # wait until the server is ON!
        worker = ZMQWorker(pusher_port) 
        worker.connect()  # connecto to distant server 
        print('%03d => connect to server' % pid)
        while keep_processing and server_status.is_set():
            try:
                data_from_server = worker.receive()
                print('worker %03d => receive : %03d metrics' % (pid, len(data_from_server)))
                metrics = data_from_server.to_dict()
                response = worker.process_data(metrics)
                print(response)
                # process dataframe 
                # ....
                # ....
                # ....
                
            except zmq.ZMQError as e: 
                pass 
    except KeyboardInterrupt as e:    
        pass 
    finally:
        print('%03d terminate ...!' % pid)
        worker.close()

@click.command()
@click.option('--pusher_port', help='port of server', default=8100)
@click.option('--source_data', help='path to source data')
@click.option('--nb_workers', help='number of workers', default=4, type=int)
def main_loop(pusher_port, source_data, nb_workers):
    try:
        server_status = mp.Event()

        server_process = mp.Process(target=server_loop, args=[pusher_port, source_data, server_status])
        workers = []
        for idx in range(nb_workers):
            wp = mp.Process(target=worker_loop, args=[pusher_port, idx, server_status])
            workers.append(wp)
            workers[-1].start()
        # end for 

        server_process.start()
    except Exception: 
        pass 

if __name__ == '__main__':
    print(' ... [processing] ... ')
    try:
        main_loop()
    except Exception as e:
        pass 
