# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:48:14 2019
@author: Derek Gloudemans

This file starts all worker processes and implements the system monitor process 
for the decentralized stream processing system. By default , 2 workers are used
and the target latency is 2 seconds. Use this file if you are disabling dummy work 
in order to use GPU for object detection
"""

import threading
import multiprocessing as mp
import queue
import numpy as np
import os
import time
import zmq
import _pickle as pickle

import matplotlib.animation as animation
from matplotlib import style
from matplotlib import pyplot as plt
import matplotlib

from worker_process import worker as worker_fn

def monitor_receiver(hosts,
                     ports,
                     monitor_message_queue,
                     timeout = 20, 
                     VERBOSE = False):
    """
    Receives output messages sent by all worker process and appends them to a mesage queue
    hosts - list of strings corresponding to worker IP addresses
    ports - list of ints corresponding to worker port numbers
    monitor_message_queue - thread-shared queue to which messages are appended
    timeout - float
    VERBOSE - bool
    """  
    
    # open subscriber socket
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
            payload = sock.recv_pyobj(zmq.NOBLOCK)
            prev_time = time.time()
            if VERBOSE: print("Monitor process received message at {}".format(time.ctime(prev_time)))
            
            # add (topic,data) to message queue
            message = pickle.loads(payload)
            if message[0] in ["heartbeat","audit_result", "task_result", "shutdown"]:
                monitor_message_queue.put(message)
        
        # no messages to receive
        except zmq.ZMQError:
            time.sleep(0.01)
            pass
      
    # close down socket
    sock.close()
    context.term()
    print ("Monitor process closed receiver socket.")


"""
This is the main code for the monitor process.
"""
if __name__ == "__main__":
    
    
    # define parameters for num workers, timeout, etc.
    num_workers = 2
    timeout = 15
    VERBOSE = False
    target_latency = 2
    
    # define ports systematically
    hosts = []
    ports = []
    audit_rate = mp.Value('f',0.1,lock = True)
    for i in range(num_workers):
        hosts.append("127.0.0.1")
        ports.append(5200+i)
    
    
    # define queue for storing messages and start monitor_receiver thread
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
        for metric in ["wait_time","latency","awt","work_time","num_processed","num_anomalies","num_restarts"]:
            performance[i][metric] = {
                    "data":[],
                    "time":[]}
            
    # define a few other structures for storing relevant data
    audits = {}
    online = np.zeros(num_workers).astype(int)
    restarts = np.zeros(num_workers).astype(int)
    anomalies = np.zeros(num_workers)
    all_audit_rates = []
    all_audit_rate_times = []
    
    
    # set up live metric plotting
    style.use('fivethirtyeight')
    plt.rcParams['animation.html'] = 'jshtml'
    matplotlib.rcParams['lines.linewidth'] = 1.0
    colors = [p['color'] for p in plt.rcParams['axes.prop_cycle']]
    fig,axs = plt.subplots(2,3,figsize = (10,15))
    fig.suptitle("Performance Monitor")
    axs[0,0].set_title("Est. wait time (heartbeat)")
    axs[0,0].set(xlabel = "Time (s)" ,ylabel = "Wait time (s)")
    axs[0,1].set_title("Actual latency")
    axs[0,1].set(xlabel = "Time (s)" ,ylabel = "Latency (s)")
    axs[0,2].set_title("Average processing time")
    axs[0,2].set(xlabel = "Time (s)" ,ylabel = "Avg. Work Time (s)")
    axs[1,0].set_title("Restarts")
    axs[1,0].set(xlabel = "Time (s)" ,ylabel = "Restarts")
    axs[1,1].set_title("Jobs completed")
    axs[1,1].set(xlabel = "Time (s)" ,ylabel = "Jobs completed")
    axs[1,2].set_title("Audit rate")
    axs[1,2].set(xlabel = "Time (s)" ,ylabel = "Audit rate")
    labels = ["Worker {}".format(i) for i in range (num_workers)]
    fig.show()
    plt.pause(0.0001)
    
    
    # main loop, in which processes will be monitored and restarted as necessary
    time.sleep(10) # to allow at least one process to return a result before plotting
    prev_time = time.time() # updated whenever a message is parsed
    prev_latency_time = prev_time # updated whenever the audit rate is adjusted
    prev_latency = 0 # for storing latency for comparison
    START_TIME = time.time() # for subtracting from stored times
    
    # while still receiving messages from workers
    while time.time() < prev_time + timeout:

        # 1. If there are any messages in the monitor_message_queue, parse 
        while True:
            try: 
                (label, payload) = monitor_message_queue.get(timeout = 0)
                prev_time = time.time()
                
                # store heartbeat info
                if label == "heartbeat": #(time gen, wait time, worker_num)
                    worker = payload[2]
                    performance[worker]["wait_time"]["data"].append(payload[1])
                    performance[worker]["wait_time"]["time"].append(payload[0]-START_TIME)
                    
                # store audit results as a hash
                elif label == "audit_result": # (worker_num,im_id,result))
                    im = payload[1]
                    worker = payload[0]
                    if im in audits.keys():
                        audits[im][worker] = hash(payload[2].tostring())
                    else:
                        audits[im] = {
                                worker:hash(payload[2].tostring())
                                }
                # store metrics from task results as well as results hash for auditing
                elif label == "task_result": # (worker_num,im_id, work_time,avg_time,latency,result,time) 
                    worker = payload[0]
                    im = payload[1]
                    
                    # store metrics
                    performance[worker]["work_time"]["data"].append(payload[2])
                    performance[worker]["work_time"]["time"].append(payload[6]-START_TIME)
                    performance[worker]["awt"]["data"].append(payload[3])
                    performance[worker]["awt"]["time"].append(payload[6]-START_TIME)
                    performance[worker]["latency"]["data"].append(payload[4])
                    performance[worker]["latency"]["time"].append(payload[6]-START_TIME)
                    performance[worker]["num_processed"]["data"].append(len(performance[worker]["latency"]["data"]))
                    performance[worker]["num_processed"]["time"].append(payload[6]-START_TIME)
                    performance[worker]["num_anomalies"]["data"].append(anomalies[worker])
                    performance[worker]["num_anomalies"]["time"].append(payload[6]-START_TIME)
                    performance[worker]["num_restarts"]["data"].append(restarts[worker])
                    performance[worker]["num_restarts"]["time"].append(payload[6]-START_TIME)

                    online[worker] = 1
                    
                    # store hash for audits
                    if im in audits.keys():
                        audits[im][worker] = hash(payload[5].tostring())
                    else:
                        audits[im] = {
                                worker:hash(payload[5].tostring())
                                }  
                        
                # note that worker is offline        
                elif label == "shutdown": # (worker_num, time)
                    worker = payload[0]
                    online[worker] = 0
                    
            # all messages dequeued
            except queue.Empty:
                break
        
        # 2. Deal with audit results
        deletions = []
        for im in audits:
            # if 3 results have been obtained for an image
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
            
            
        # 3. Check for unresponsive processes        
        # check each process to make sure a heartbeat has been received within 2 x average work time
        for worker_num in performance:
            if online[worker_num]:
                awt = performance[worker_num]["awt"]['data'][-1] # get most recent awt
                last_heartbeat_time = performance[worker_num]["wait_time"]["time"][-1]
                
                if last_heartbeat_time + awt*2 +10 < time.time()-START_TIME:
                    anomalies[worker_num] += 3
        
        ### Enable for massive worker death at t = 60
        if False:
            if time.time() > START_TIME +120 and time.time() < START_TIME + 125:
                for i in range (len(anomalies)):
                    anomalies[i] = 3
                    
        ### Enable for stochastic worker death
        if False:
            for i in range(len(anomalies)):
                out = np.random.rand()
                if out < 0.05:
                    anomalies[i] = 3
                    
        # 4. for any process, if 3 anomalies have been recorded, restart it
        for worker_num in range(len(anomalies)):
            
            if online[worker_num] and anomalies[worker_num] >= 3:
                
                anomalies[worker_num] = 0
                online[worker_num] = 0
                restarts[worker_num] += 1
                
                worker_processes[worker_num].terminate()
                p = mp.Process(target = worker_fn, args = (hosts,ports,audit_rate,worker_num,timeout,VERBOSE,False))
                p.start()
                
                worker_processes[worker_num] = p
                print("System monitor restarted worker {} at {}.".format(worker_num, time.ctime(time.time())))
        
        
        # 5. Adjust audit request ratio to move towards target latency
        # get average latency across all workers
        avg_latency = 0
        for worker_num in performance:
            if online[worker_num]:
                avg_latency += performance[worker_num]["latency"]["data"][-1]
        if sum(online) > 0:
            avg_latency = avg_latency / sum(online)
            prev_latency = avg_latency
        else:
            # in the case when no workers are online, don't want a divide by zero error
            avg_latency = prev_latency 
            
        # adjust audit ratio to reach target_latency every 5 seconds
        if time.time() > prev_latency_time + 5: # print every 5 seconds
            prev_latency_time = time.time()
            if avg_latency > target_latency:
                # bump audit rate down
                with audit_rate.get_lock():
                    audit_rate.value = max(audit_rate.value * 0.95,0.01)
                    audit_val = audit_rate.value
                    all_audit_rates.append(audit_val)
                    all_audit_rate_times.append(time.time()-START_TIME)
            else:
                # bump audit rate up
                with audit_rate.get_lock():
                    audit_rate.value = min(audit_rate.value * 1.05,1)
                    audit_val = audit_rate.value
                    all_audit_rates.append(audit_val)
                    all_audit_rate_times.append(time.time()-START_TIME)
    
            # 6. Output performance metrics
            print("================= Monitor Summary ==================")
            for i in range(0,len(online)):
                status =  "Online" if online[i] else "Offline"
                print("Worker {} status: {}".format(i,status))
                if online[i]:
                    print("Current wait time: {}".format(performance[i]['wait_time']['data'][-1]))
                    print("Average work time: {}".format(performance[i]['awt']['data'][-1]))
                    print("Most recent latency: {}".format(performance[i]['latency']['data'][-1]))
                    print("Num images processed: {}".format(len(performance[i]['work_time']['data'])))
                    print("Num restarts: {}".format(restarts[i]))
                    print("--------------------")
                    
            print("Current latency: {}. Adjusted audit rate to {}".format(avg_latency,audit_val))   
            print("====================================================")
        
        # update plot    
        handles = []
        for worker in performance:         
            axs[0,0].plot(performance[worker]['wait_time']['time'][-100:],performance[worker]['wait_time']['data'][-100:],color = colors[worker])
            axs[0,1].plot(performance[worker]['latency']['time'][-100:],performance[worker]['latency']['data'][-100:],color = colors[worker])
            axs[0,2].plot(performance[worker]['awt']['time'][-100:],performance[worker]['awt']['data'][-100:],color = colors[worker])
            axs[1,0].plot(performance[worker]['num_restarts']['time'][-100:],performance[worker]['num_restarts']['data'][-100:],color = colors[worker])
            out = axs[1,1].plot(performance[worker]['num_processed']['time'][-100:],performance[worker]['num_processed']['data'][-100:],color = colors[worker])
            handles.append(out[0])
        axs[1,2].plot(all_audit_rate_times,all_audit_rates, color = 'k')        
        fig.legend(handles, labels, loc='lower right')
        fig.canvas.draw()
        plt.pause(0.0001)
    
    # finally, wait for all worker processes to close
    print("========================Monitor Process exited.================================")
    for p in worker_processes:
        p.join()
    print("All worker processes terminated")    
    
    t_receiver.join()