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

from worker_process import worker as worker_fn

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
    target_latency = 7
    
    hosts = []
    ports = []
    audit_rate = mp.Value('f',0.1,lock = True)
    for i in range(num_workers):
        hosts.append("127.0.0.1")
        ports.append(5200+i)
    
    
    # define queue for storing results grabbed by monitor_receiver thread
    monitor_message_queue = queue.Queue()
    t_receiver = threading.Thread(target = monitor_receiver, args = 
                                 (hosts,
                                 ports,
                                 monitor_message_queue,
                                 timeout,
                                 VERBOSE,))
    t_receiver.start()
    

    # start processes initially
    worker_processes = []
    for i in range(num_workers):
        p = mp.Process(target = worker_fn, args = (hosts,ports,audit_rate,i,timeout,VERBOSE,))
        p.start()
        worker_processes.append(p)
    print("All worker processes started")
    
    
    # define nested dictionary structure for storing metrics
    performance = {}
    for i in range(num_workers):
        performance[i] = {}
        for metric in ["wait_time","latency","awt","work_time"]:
            performance[i][metric] = {
                    "data":[],
                    "time":[]}
    audits = {}
    
    online = np.zeros(num_workers)
    anomalies = np.zeros(num_workers)
    
    # main loop, in which processes will be monitored and restarted as necessary
    time.sleep(10)
    prev_time = time.time()
    prev_latency_time = prev_time
    while time.time() < prev_time + timeout:
        
        # 1. If there are any messages in the monitor_message_queue, parse 
        while True:
            try: 
                (label, payload) = monitor_message_queue.get(timeout = 0)
                prev_time = time.time()
                
                if label == "heartbeat": #(time gen, wait time, worker_num)
                    worker = payload[2]
                    performance[worker]["wait_time"]["data"].append(payload[1])
                    performance[worker]["wait_time"]["time"].append(payload[0])
                    
                elif label == "audit_result": # (worker_num,im_id,result))
                    im = payload[1]
                    worker = payload[0]
                    if im in audits.keys():
                        audits[im][worker] = hash(payload[2].tostring())
                    else:
                        audits[im] = {
                                worker:hash(payload[2].tostring())
                                }
                        
                elif label == "task_result": # (worker_num,im_id, work_time,avg_time,latency,result,time) 
                    worker = payload[0]
                    im = payload[1]
                    
                    # store metrics
                    performance[worker]["work_time"]["data"].append(payload[2])
                    performance[worker]["work_time"]["time"].append(payload[6])
                    performance[worker]["awt"]["data"].append(payload[3])
                    performance[worker]["awt"]["time"].append(payload[6])
                    performance[worker]["latency"]["data"].append(payload[4])
                    performance[worker]["latency"]["time"].append(payload[6])
                    online[worker] = 1
                    
                    # store hash for audits
                    if im in audits.keys():
                        audits[im][worker] = hash(payload[5].tostring())
                    else:
                        audits[im] = {
                                worker:hash(payload[5].tostring())
                                }  
                        
            # all messages dequeued
            except queue.Empty:
                break
    
        # 2. Plot performance metrics
        # TODO
        
        # 3. Deal with audit results
        deletions = []
        for im in audits:
            if len(audits[im]) == 3:
                deletions.append(im)
                all_hashes = []
                for worker in audits[im]:
                    all_hashes.append(audits[im][worker])
                # get most common hash
                hash_dict = {}
                for hash_val in all_hashes:
                        if hash_val in hash_dict.keys():
                            hash_dict[hash_val] +=1
                        else:
                            hash_dict[hash_val] = {}
                            hash_dict[hash_val] = 1
                
                # count hashes
                most_common_hash = None
                count = 0
                for hash_val in hash_dict:
                    if hash_dict[hash_val] > count:
                        count = hash_dict[hash_val]
                        most_common_hash = hash_val
                # record anomalous results
                for worker_num in audits[im]:
                    if audits[im][worker_num] != most_common_hash:
                        anomalies[worker_num] += 1
                        
        # remove all finished audits from audit dict
        for im in deletions:
            del audits[im]
            
            
        # 4. Check for unresponsive processes        
        # check each process to make sure a heartbeat has been received within 2 x average work time
        for worker_num in performance:
            if online[worker_num]:
                awt = performance[worker_num]["awt"]['data'][-1] # get most recent awt
                last_heartbeat_time = performance[worker_num]["wait_time"]["time"][-1]
                
                if last_heartbeat_time + 2*awt < time.time():
                    anomalies[worker_num] += 1
                
        # 5. for any process, if 3 anomalies have been recorded, restart it
        for worker_num in range(len(anomalies)):
            if anomalies[worker_num] >= 3:
                worker_processes[worker_num].terminate()
                
                p = mp.Process(target = worker_fn, args = (hosts,ports,audit_rate,worker_num,timeout,VERBOSE,))
                p.start()
                worker_processes[worker_num] = p
                print("System monitor restarted worker {} at {}.".format(worker_num, time.time()))
        
        
        # 6. Adjust audit request ratio to move towards target latency
        # get average latency across all workers
        avg_latency = 0
        for worker_num in performance:
            if online[worker]:
                avg_latency += performance[worker_num]["latency"]["data"][-1]
        avg_latency = avg_latency / sum(online)
        
        # adjust audit ratio to reach target_latency every 5 seconds
        if time.time() > prev_latency_time + 5: # print every 5 seconds
            prev_latency_time = time.time()
            if avg_latency > target_latency:
                with audit_rate.get_lock():
                    audit_rate.value = max(audit_rate.value * 0.95,0.01)
                    audit_val = audit_rate.value
            else:
                with audit_rate.get_lock():
                    audit_rate.value = min(audit_rate.value * 1.05,1)
                    audit_val = audit_rate.value
            print("Current latency: {}. Adjusted audit rate to {}".format(avg_latency,audit_val))   
    
    # finally, wait for all worker processes to close
    for p in worker_processes:
        p.join()
    print("All worker processes terminated")    
    
    t_receiver.join()