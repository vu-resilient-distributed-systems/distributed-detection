# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:48:14 2019
@author: Derek Gloudemans

This file contains the function that creates a worker process
"""
import threading
import multiprocessing as mp
import queue
import numpy as np
import os
import time
import zmq
import _pickle as pickle

from worker_process import worker

def monitor_receiver(hosts,
                             ports,
                             monitor_message_queue,
                             timeout = 20, 
                             VERBOSE = False):
    """
    NEED COMMENTS HERE
    """    
    # open publisher socket
    context = zmq.Context()
    sock = context.socket(zmq.SUB)
    for i in range(len(ports)):
        host = hosts[i]
        port = ports[i]
        sock.connect("tcp://{}:{}".format(host, port))
        sock.subscribe(b'')
        
    # main receiving loop
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            #topic = sock.recv_string(zmq.NOBLOCK)
            payload = sock.recv_pyobj(zmq.NOBLOCK)
            prev_time = time.time()
            if VERBOSE: print("Monitor process received message at {}".format(time.ctime(prev_time)))
            
            # add (topic,data) to message queue
            message = pickle.loads(payload)
            if message[0] in ["heartbeat","audit_result", "task_result"]:
                monitor_message_queue.put(message)
        
        except zmq.ZMQError:
            time.sleep(0.01)
            pass
        
    sock.close()
    context.term()
    print ("Monitor process closed receiver socket.")
  
if __name__ == "__main__":
    
    
    # define parameters for num workers, timeout, etc.
    num_workers = 4
    timeout = 30
    VERBOSE = False
    
    hosts = []
    ports = []
    audit_rate = mp.Value('f',0.1,lock = True)
    for i in range(num_workers):
        hosts.append("127.0.0.1")
        ports.append(5200+i)
    
    
    # define queue for storing results grabbed by monitor_receiver thread
    monitor_message_queue = queue.Queue()
    t_monitor = threading.Thread(target = monitor_receiver, args = 
                                 (hosts,
                                 ports,
                                 monitor_message_queue,
                                 timeout,
                                 VERBOSE,))
    t_monitor.start()
    
    # start processes initially
    worker_processes = []
    for i in range(num_workers):
        p = mp.Process(target = worker, args = (hosts,ports,audit_rate,i,timeout,VERBOSE,))
        p.start()
        worker_processes.append(p)
    print("All worker processes started")
    
    
    # main loop, in which processes will be monitored and restarted as necessary
    prev_time = time.time()
    while time.time() < prev_time + timeout:
        
        # 1. If there are any messages in the monitor_message_queue, parse 
        while True:
            try: 
                (label, payload) = monitor_message_queue.get(timeout = 0)
                
                if label == "heartbeat":
                    pass
                elif label == "audit_result":
                    pass
                elif label == "task_result":
                    pass
                
            except queue.Empty:
                break
    
        # 2. Plot performance metrics
        
        # 3. Deal with audit results
        
        # 4. Deal with slow or unresponsive processes
        
        # 5. Adjust audit request ratio to move towards target latency
    
    
    # finally, wait for all worker processes to close
    for p in worker_processes:
        p.join()
    print("All worker processes terminated")    
    
    t_monitor.join()