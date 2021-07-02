import click 

import cv2 
import zmq 
import pickle 
import numpy as np 
from time import sleep 

from collections import Counter 

import multiprocessing as mp 
from libraries.strategies import ZMQServer, ZMQWorker, create_screen


def server_loop(pusher_port, source_data, server_status):
    try:
        server = ZMQServer(pusher_port, source_data)
        server.start()  # brancher le server sur le port 
        server_status.set()
        keep_sending = True 
        while keep_sending and not server.empty():
            server.send()
            sleep(0.5)  # wait 1 second 
    except KeyboardInterrupt as e:
        pass
    finally:
        server_status.clear()  # turn off the server 
        sleep(5)  # 3 seconds 
        server.close()

def worker_loop(pusher_port, pid, server_status, shared_queue):
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
                gender = int(response['sex']['M'] > response['sex']['F']) 
                shared_queue.put((pid, gender))
            except zmq.ZMQError as e: 
                pass 
    except KeyboardInterrupt as e:    
        pass 
    finally:
        print('%03d terminate ...!' % pid)
        worker.close()


def sink_loop(source, shared_queue):
    W, H = 640, 480
    s00 = '000'
    s01 = '001'
    create_screen(s00, W, H, (100, 100))
    create_screen(s01, W, H, (800, 100))
    
    capture = cv2.VideoCapture(source)
    keep_capture = True 
    while keep_capture:
        read_status, bgr_frame = capture.read() 
        key_code = cv2.waitKey(25) & 0xFF 
        keep_capture = key_code != 27 and read_status
        if keep_capture:
            resized_frame = cv2.resize(bgr_frame, (640, 480))
            edged_frame = cv2.Canny(resized_frame, 50, 80)
            if not shared_queue.empty():
                pid, gender = shared_queue.get()
                print(pid, '===>', gender)
                if gender == 0: 
                    cv2.imshow(s00, cv2.cvtColor(resized_frame, cv2.COLOR_BGR2GRAY))
                else:
                    cv2.imshow(s00, resized_frame)
                cv2.imshow(s01, edged_frame)
    # end loop 
    capture.release()
    cv2.destroyAllWindows()

@click.command()
@click.option('--source', help='path to video soruce data')
@click.option('--pusher_port', help='port of server', default=8100)
@click.option('--source_data', help='path to source data')
@click.option('--nb_workers', help='number of workers', default=4, type=int)
def main_loop(source, pusher_port, source_data, nb_workers):
    try:
        shared_queue = mp.Queue()
        server_status = mp.Event()
        server_process = mp.Process(target=server_loop, args=[pusher_port, source_data, server_status])
        workers = []
        for idx in range(nb_workers):
            wp = mp.Process(
                target=worker_loop, 
                args=[pusher_port, idx, server_status, shared_queue]
            )
            workers.append(wp)
            workers[-1].start()
        # end for 
        server_process.start()
        sink_process = mp.Process(target=sink_loop, args=[source, shared_queue])
        sink_process.start()
    except Exception: 
        pass 

if __name__ == '__main__':
    print(' ... [processing] ... ')
    try:
        main_loop()
    except Exception as e:
        pass 
