# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:48:14 2019
@author: Derek Gloudemans

This file contains the functions executed by each of the threads in the worker
process
"""

import zmq
import socket
import random
import time
import queue
import _pickle as pickle
import multiprocessing as mp
import threading
import numpy as np
import os
import csv

from simple_yolo.yolo_detector import Darknet_Detector

def receive_images(p_image_queue,p_new_im_id_queue, host = "127.0.0.1", port = 6200, timeout = 20, VERBOSE = True,worker_num = 0):
    """
    Creates a ZMQ socket and listens on specified port, receiving and writing to
    shared image queue the received images
    p_image_queue - queue created by worker process and shared among threads 
                    for storing received images
    host - string - the host IP address, default is local host
    port - int - port num
    """
    
    # open ZMQ socket
    context = zmq.Context()
    sock = context.socket(zmq.SUB)
    sock.connect("tcp://{}:{}".format(host, port))
    sock.subscribe(b'') # subscribe to all topics on this port (only images)
    
    print ("w{}: Image receiver thread connected to socket.".format(worker_num))
    
    # main receiving loop
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            temp = sock.recv_pyobj(zmq.NOBLOCK)
            (name,im) = pickle.loads(temp)
            p_image_queue.put((name,im,time.time())) 
            p_new_im_id_queue.put(name)
            prev_time = time.time()
            if VERBOSE: print("w{}: Image receiver thread received image {} at {}".format(worker_num,name,time.ctime(prev_time)))
        except zmq.ZMQError:
            time.sleep(0.1)
            pass
        
    sock.close()
    print ("w{}: Image receiver thread closed socket.".format(worker_num))


def query_handler(host,
                  port,
                  p_message_queue,
                  p_query_requests,
                  p_query_results,
                  p_database_lock,
                  query_timeout = 5,
                  timeout = 20,
                  VERBOSE = True,
                  worker_num = 0):
    """
    Has four main steps in main loop:
        1.  Receives query requests via UDP port specified by host and port, and
            forwards the requests to all other workers via message queue
        host - string
        port - int
        2.  Upon receiving a query request forwarded from another worker (in p_query_request)
            accesses own database and returns the requested data via message queue
        p_query_requests - thread-shared queue object
        p_message_queue - thread-shared queue object
        3. Parses all return values from p_query_result
        p_query_results - thread-shared queue object
        4. after timeout period, returns the most common value to the client via message queue
           i.e. it is assumed client is subscribed to "query_output" from this worker's sender port
           Also updates own database with this most common value
        timeout - float - seconds
    """
    
    # specify database file path
    data_file = os.path.join("databases","worker_{}_database.csv".format(worker_num))
    
    # open UDP socket to receive requests
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    sock.setblocking(0) 
    print("w{}: Query thread opened receiver socket.".format(worker_num))
    
    active_queries = {}
    
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        # 1. receive new external queries
        try:
            # receive query
            queried_im_id = int(sock.recv(1024).decode('utf-8'))
            prev_time = time.time()
            
            # add query to dict of active queries
            active_queries[queried_im_id] = {"time_in": prev_time,
                                         "vals": [get_im_data(data_file,queried_im_id,p_database_lock)[0]]}
            
            # forward query to all other workers via message queue
            # the False indicates that this is not an internal request
            message = ("query_request", (queried_im_id,worker_num,False))
            p_message_queue.put(message)
            if VERBOSE: print("w{}: Requested query for im {}.".format(worker_num,queried_im_id))
        except BlockingIOError:# socket.timeout:
            pass
    
        # 2. respond to all query requests
        while True:
            try:
                # get request from queue - both consistency and external query
                # requests are handled here, and INTERNAL indicates which type 
                # this request is so it can be returned to the correct thread
                (requested_im_id,request_worker_num,INTERNAL) = p_query_requests.get(timeout = 0)
                prev_time = time.time()
                
                # helper function returns relevant numpy array and num_validators or None,0
                data = get_im_data(data_file,requested_im_id,p_database_lock)[0]
                
                # send data back via message queue
                if INTERNAL:
                    message = ("consistency_result",(data,requested_im_id,request_worker_num))
                else:
                    message = ("query_result",(data,requested_im_id,request_worker_num))
                p_message_queue.put(message)  
                if VERBOSE: print("w{}: Responded to query request im {} for worker {}.".format(worker_num,requested_im_id,request_worker_num))
           
            except queue.Empty:
                break
#            except PermissionError:
#                print("---------------------worker {} permission error.".format(worker_num))
            
        # 3. parse query results
        while True:
            try:
                (query_data,query_im_id) = p_query_results.get(timeout = 0) 
                prev_time = time.time()
                # add if still active
                if query_im_id in active_queries.keys():
                    active_queries[query_im_id]["vals"].append(query_data)  
                if VERBOSE: print("w{}: Parsed query response for im {}.".format(worker_num,query_im_id))
            except queue.Empty:
                break
               
        # 4.cycle through active queries and return result for all that have timed out
        timed_out = []
        for id_tag in active_queries:
            prev_time = time.time()
            query = active_queries[id_tag]
            if query['time_in'] + query_timeout < time.time():
                # get most common val by comparing unique hashes
                hash_dict = {}
                for data in query["vals"]:
                    if type(data) == np.ndarray: # make sure None doesn't become the most common value
                        data_hash = hash(data.tostring())
                        if data_hash in hash_dict.keys():
                            hash_dict[data_hash]["count"] +=1
                        else:
                            hash_dict[data_hash] = {}
                            hash_dict[data_hash]["count"] = 1
                            hash_dict[data_hash]["data"] = data
                
                # count hashes
                most_common_data = None
                count = 0
                for data_hash in hash_dict:
                    if hash_dict[data_hash]["count"] > count:
                        count = hash_dict[data_hash]["count"]
                        most_common_data = hash_dict[data_hash]["data"]
        
                # lastly, compare to own data and see if count is greater than
                # num_validators on previous data. If not, send own value and 
                # don't update own value
                (own_data, own_num_validators) = get_im_data(data_file,id_tag,p_database_lock)
                if own_num_validators >= count:
                    message = ("query_output", (id_tag,own_data))
                else:
                    message = ("query_output", (id_tag,most_common_data))
                    # update database
                    update_data(data_file,count,most_common_data,p_database_lock)
                    
                # output query overall result
                p_message_queue.put(message)
                print("w{}: Output query result for im {}.".format(worker_num,id_tag))
                timed_out.append(id_tag)
          
        # remove all handled requests
        timed_out.reverse()
        for tag in timed_out:
            del active_queries[tag]
                
    # end of main while loop
    sock.close()
    print("w{}: Query thread exited.".format(worker_num))                
            

def consistency_function(p_message_queue,
                         p_consistency_results,
                         p_last_balanced,
                         p_database_lock,
                         p_continue_consistency,
                         consistency_rate = 2, # queries per second
                         query_timeout = 5,
                         timeout = 20,
                         VERBOSE = False,
                         worker_num = 0):
    """
    Systematically sends queries in on each image ID to all workers, and uses the 
    most common returned value to update own database. 
    """
    print("w{}: Consistency thread started.".format(worker_num))
    
    # path to worker's database
    data_file = os.path.join("databases","worker_{}_database.csv".format(worker_num))
    
    # wait until there are data results in database
    next_im_id = -1
    while next_im_id < 5:
        time.sleep(0.1)
        with p_last_balanced.get_lock():
            next_im_id = p_last_balanced.value
    
    prev_time = time.time()
    active_queries = {}    
    
    with p_continue_consistency.get_lock():
        continue_val = p_continue_consistency.value
        
    while continue_val:
        
        if time.time() > prev_time + 1/consistency_rate:
            
            # cycle backwards through im_ids
            next_im_id -= 1
            if next_im_id < 0:
                with p_last_balanced.get_lock():
                    next_im_id = p_last_balanced.value
            # add query to dict of active queries
            active_queries[next_im_id] = {"time_in": time.time(),
                                             "vals": [get_im_data(data_file,next_im_id,p_database_lock)[0]]}
            
            # forward consistency query to all other workers via message queue
            # the True indicates that this is an internal request
            message = ("query_request", (next_im_id,worker_num,True))
            p_message_queue.put(message)
            #if VERBOSE: print("w{}: Consistency query for im {} requested.".format(worker_num,next_im_id))
        
            # parse results from consistency results queue
            while True:
                try:
                    (query_data,query_im_id) = p_consistency_results.get(timeout = 0) 
                    prev_time = time.time()
                    # add if still active
                    if query_im_id in active_queries.keys():
                        active_queries[query_im_id]["vals"].append(query_data)  
                    #if VERBOSE: print("w{}: Parsed consistency response for im {}.".format(worker_num,query_im_id))
                except queue.Empty:
                    break
                
            # cycle through active queries and return result for all that have timed out
            timed_out = []
            for id_tag in active_queries:
                prev_time = time.time()
                query = active_queries[id_tag]
                if query['time_in'] + query_timeout < time.time():
                    # get most common val by comparing unique hashes
                    hash_dict = {}
                    for data in query["vals"]:
                        if type(data) == np.ndarray: # make sure None doesn't become the most common value
                            data_hash = hash(data.tostring())
                            if data_hash in hash_dict.keys():
                                hash_dict[data_hash]["count"] +=1
                            else:
                                hash_dict[data_hash] = {}
                                hash_dict[data_hash]["count"] = 1
                                hash_dict[data_hash]["data"] = data
                    
                    # count hashes
                    most_common_data = None
                    count = 0
                    for data_hash in hash_dict:
                        if hash_dict[data_hash]["count"] > count:
                            count = hash_dict[data_hash]["count"]
                            most_common_data = hash_dict[data_hash]["data"]
            
                    # lastly, compare to own data and see if count is greater than
                    # num_validators on previous data. If not, send own value and 
                    # don't update own value
                    (own_data, own_num_validators) = get_im_data(data_file,id_tag,p_database_lock)
                    if own_num_validators < count:
                        assert len(most_common_data[0]) > 0, print("most_common_data isn't valid")
                        update_data(data_file,count,most_common_data,p_database_lock)
                        if VERBOSE: print("w{}: Consistency update on im {} with {} validators.".format(worker_num,id_tag,count))
                    
                    timed_out.append(id_tag)
              
            # remove all handled requests
            timed_out.reverse()
            for tag in timed_out:
                del active_queries[tag]
                
            # determine whether to continue using shared variable with heartbeat thread
            with p_continue_consistency.get_lock():
                continue_val = p_continue_consistency.value
            
    print("{}: Consistency thread exited.".format(worker_num))
    
    

def send_messages(host,port,p_message_queue, timeout = 20, VERBOSE = False,worker_num = 0):
    """
    Repeatedly checks p_message_queue for messages and sends them to all other 
    workers. It is assumed that the other processes prepackage information so all
    this function has to do is send the information. messages are of the form:
        (topic, (pickled message payload))
    worker_addresses - list of tuples (host_address (string), host port (int))
    p_message_queue - shared queue amongst all worker threads
    """    
    
    # open publisher socket
    context = zmq.Context()
    sock = context.socket(zmq.PUB)
    sock.bind("tcp://{}:{}".format(host, port))
    time.sleep(3) # pause to allow subscribers to connect
    
    # sending loop
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            message = p_message_queue.get(timeout = timeout)
            payload = pickle.dumps(message)
            #sock.send_string(topic)
            sock.send_pyobj(payload)
            prev_time = time.time()
            if VERBOSE: print("w{}: Sender thread sent message at {}".format(worker_num,time.ctime(prev_time)))
            
            
        except queue.Empty:
            time.sleep(0.1)
    
    sock.close()
    context.term()
    print ("w{}: Message sender thread closed socket.".format(worker_num))


def receive_messages(hosts,
                     ports,
                     p_lb_queue,
                     p_audit_buffer,
                     p_query_requests,
                     p_query_results, 
                     p_consistency_results,
                     timeout = 20, 
                     VERBOSE = False,
                     worker_num = 0):
    """
    Repeatedly checks p_message_queue for messages and sends them to all other 
    workers. It is assumed that the other processes prepackage information so all
    this function has to do is send the information. messages are of the form:
        (topic, (unpickled message payload))
    worker_addresses - list of tuples (host_address (string), host port (int))
    p_message_queue - shared queue amongst all worker threads
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
            if VERBOSE: print("{}: Receiver thread received message at {}".format(worker_num,time.ctime(prev_time)))
            # parse topic and payload accordingly here
            (label,data) = pickle.loads(payload)
            
            # deal with different types of messages (different label field)
            if label == "heartbeat":
                # (time heartbeat generated, workers wait time, worker_num)
                p_lb_queue.put(data)
            
            # deal with audit requests
            elif label == "audit_request":
                # send audit_requests to two other 
                (im_id,auditors) = data
                if worker_num in auditors:
                    p_audit_buffer.put(im_id)
                    
            elif label == "query_request":
                # data = (queried_im_id,worker_num,INTERNAL)
                p_query_requests.put(data)
                
            elif label == "query_result":
                # data = (queried_data, query_im_id,destination_worker)
                if data[2] == worker_num:
                    p_query_results.put(data[0:2]) # since destination worker not necessary
                
            elif label == "consistency_result":
                # data = (queried_data, query_im_id,destination_worker)
                if data[2] == worker_num:
                    p_consistency_results.put(data[0:2]) # since destination worker not necessary
            
            else:
                pass
        
        except zmq.ZMQError:
            time.sleep(0.1)
            pass
        
    sock.close()
    context.term()
    print ("w{}: Message receiver thread closed socket.".format(worker_num))


def heartbeat(p_average_time,
              p_message_queue,
              p_num_tasks,
              p_continue_consistency,
              interval = 0.5,
              timeout = 20, 
              VERBOSE = False,
              worker_num = 0):
    """
    Sends out as a heartbeat the average wait time at regular intervals
    p_average_time - shared variable for average time,
    """

    # get average time
    with p_average_time.get_lock():
        avg_time = p_average_time.value
    #get num_tasks
    with p_num_tasks.get_lock():
        num_tasks = p_num_tasks.value
    
    
    last_wait = avg_time* (num_tasks+1)
    prev_time = time.time()
    
    while prev_time + 2*timeout > time.time():
        # get average time
        with p_average_time.get_lock():
            avg_time = p_average_time.value
        #get num_tasks
        with p_num_tasks.get_lock():
            num_tasks = p_num_tasks.value
            
        cur_wait = avg_time * (num_tasks+1)
        
        # send message
        message = ("heartbeat",(time.time(),cur_wait,worker_num))
        p_message_queue.put(message)
        
        if VERBOSE: print("w{}: Heartbeat thread added '{:.2f}s' to message queue.".format(
                worker_num,cur_wait))
        
        # updates prev_time whenever wait time changes
        if cur_wait != last_wait:
            prev_time = time.time() 
            last_wait = cur_wait
        
        # a bit imprecise since the other operations in the loop do take some time
        time.sleep(interval)
    
    # tell consistency thread to exit
    with p_continue_consistency.get_lock():
         p_continue_consistency.value = False
    
    # send message to monitor process indicating worker is shutting down gracefully
    message = ("shutdown",(worker_num, time.time()))
    
    print("w{}: Heartbeat thread exited.".format(worker_num))
    


def load_balance(p_new_image_id_queue,
                 p_task_buffer,
                 p_lb_results,
                 p_message_queue,
                 p_average_time,
                 p_num_tasks,
                 p_last_balanced,
                 audit_rate,
                 timeout = 20,
                 lb_timeout = 2, 
                 VERBOSE = True,
                 num_workers = 1,
                 worker_num = 0):
    """
    Every time a new image is added to the new_image_id_queue, sends worker's 
    current estimated wait time to all other workers, waits for timeout, and 
    compares current estimated wait times to those of all workers received after
    the message was sent. Adds image to task list if worker has min wait time
    """
    prev_time = time.time()
    all_received_times = {}
    
    while time.time() - prev_time < timeout:
        # if there is a new image id
        try:
            # grabs new image id from queue if there is one
            im_id = p_new_image_id_queue.get(timeout = 0)
            time_received = time.time()
            
            # get all pending times in p_lb_results and append to all_received_times
            while True:
                try: 
                    # (time heartbeat generated, workers wait time, worker_num)
                    message = p_lb_results.get(timeout = 0)
                    # if message is more recent than previously stored
                    # will throw error if there is no entry for that worker num yet
                    try:
                        if message[0] > all_received_times[message[2]][0]:
                            all_received_times[message[2]] = (message[0],message[1])
                    except KeyError:
                            all_received_times[message[2]] = (message[0],message[1])
                except queue.Empty:
                    break

            # go through all_received times and remove all messages more than lb_timeout old
            # also keep running track of min valid time
            min_time = np.inf
            deletions = []
            for worker in all_received_times: 
                # each item is of form (time_generated, wait time, worker_num)
                (other_gen_time, other_wait_time) = all_received_times[worker]
                
                # check if item is out of date
                if other_gen_time + lb_timeout < time_received:
                    deletions.append(worker)
                else:
                    if other_wait_time < min_time:
                        min_time = other_wait_time
            
            #delete out of date items
            deletions.reverse()
            for i in deletions:
                del all_received_times[i]

            # get average time
            with p_average_time.get_lock():
                avg_time = p_average_time.value
            #get num_tasks
            with p_num_tasks.get_lock():
                num_tasks = p_num_tasks.value
            cur_wait = avg_time* (num_tasks + 1)
            
            #print("Cur_wait time: {} Min time: {}".format(cur_wait,min_time))
            
            # this worker has minimum time to process, so add to task queue
            if min_time >= cur_wait:
                p_task_buffer.put(im_id)
                if VERBOSE: print("w{}: Load balancer added {} to task list. Est wait: {:.2f}s".format(worker_num,im_id,cur_wait))

                # randomly audit with audit_rate probability
                with audit_rate.get_lock():
                    audit_rate_val = audit_rate.value
                
                if random.random() < audit_rate_val:

                    # get all worker nums besides this worker's
                    worker_nums = [i for i in range(num_workers)]
                    worker_nums.remove(worker_num)
                    random.shuffle(worker_nums)
                    
                    # get up to two auditors if there are that many other workers
                    auditors = []
                    if len(worker_nums)>0:
                        auditors.append(worker_nums[0])
                        if len(worker_nums) > 1:
                            auditors.append(worker_nums[1])
                            
                    # send audit request message
                    message = ("audit_request", (im_id,auditors))
                    p_message_queue.put(message)
                    if VERBOSE: print("w{}: LB requested audit of image {} from {}".format(worker_num, im_id,auditors))
            
            # get average time
            with p_last_balanced.get_lock():
                p_last_balanced.value = im_id    
                
            prev_time = time_received
            
        # there are no items in new_image_id_queue
        except queue.Empty:
            time.sleep(0.1)
            pass
                
    print("w{}: Load balancer thread exited.".format(worker_num))
 
def work_function(p_image_queue,
                  p_task_buffer,
                  p_audit_buffer,
                  p_message_queue,
                  p_average_time,
                  p_num_tasks,
                  p_last_balanced,
                  p_database_lock,
                  timeout = 20, 
                  VERBOSE = True,
                  worker_num = 0):
    """
    -Repeatedly gets first image from image_queue. If in audit_buffer or task_buffer,
    processes image. Otherwise, discards image and loads next.
    -Processing an image consists of performing object detection on the image and
    outputting the results (numpy array)
    -If task, the results are written to a local data file, and processing speed, latency, and average processing speed are reported to monitor process
    -If audit, full results are sent to monitor process and are not written to file

    """ 
    
    # create network for doing work
    model = Darknet_Detector('simple_yolo/cfg/yolov3.cfg',
                                'simple_yolo/yolov3.weights',
                                'simple_yolo/data/coco.names',
    
                            'simple_yolo/pallete')
    audit_list = [] # will store all im_ids taken from p_audit_buffer
    task_list = [] # will store all im_ids taken from p_task_buffer
    
    # initialize loop
    count = 0
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            # get image off of image_queue
            (im_id,image,im_time_received) = p_image_queue.get(timeout = 0)
            
            # get average time
            with p_last_balanced.get_lock():
                last_balanced = p_last_balanced.value  
                
            # wait until the last balanced image is at least equal with im_id
            while last_balanced < im_id:
                time.sleep(0.01)
                with p_last_balanced.get_lock():
                    last_balanced = p_last_balanced.value  
            
            # update audit_list and task_list
            while True:
                try: 
                    audit_id = p_audit_buffer.get(timeout = 0)
                    audit_list.append(audit_id)
                except queue.Empty:
                    break
            while True:
                try: 
                    task_id = p_task_buffer.get(timeout = 0)
                    task_list.append(task_id)
                except queue.Empty:
                    break
            
            # update p_num_tasks
            with p_num_tasks.get_lock():
                p_num_tasks.value = int(len(task_list) + len(audit_list))
            
            # check if audit task, normal task, or neither
            AUDIT = False
            TASK = False
            if im_id in task_list:
                TASK = True
                task_list.remove(im_id)
            if im_id in audit_list:
                AUDIT = True
                audit_list.remove(im_id)
                
            if AUDIT or TASK:
                #if VERBOSE: print("w{} work began processing image {}.".format(worker_num, im_id))
                
                ############## DO WORK ############## 
                work_start_time = time.time()
                dummy_work = True
                if dummy_work:
                    result = np.ones([10,8])
                    if worker_num == 0:
                        result = np.zeros([10,8])
                    time.sleep(max((0,np.random.normal(3,1))))            
                else:
                    result, _ = model.detect(image).data.numpy()
                
                work_end_time = time.time()
                prev_time = time.time()
                
                # if audit, write results to monitor process
                if AUDIT:
                    # package message
                    message = ("audit_result",(worker_num,im_id,result))
                    p_message_queue.put(message)
                    if VERBOSE: print("w{}: Work audit results on image {} sent to message queue".format(worker_num, im_id))
                # if task, write results to database and report metrics to monitor process
                if TASK:
                    # write results to database
                    data_file = os.path.join("databases","worker_{}_database.csv".format(worker_num))
                    write_data_csv(data_file,result,im_id,p_database_lock)

                    # compute metrics
                    latency = work_end_time - im_time_received
                    proc_time = work_end_time - work_start_time
                    
                    # update average time with weighting of current speed at 0.2
                    with p_average_time.get_lock():
                        avg_time = p_average_time.value*0.9 + 0.1* proc_time
                        p_average_time.value = avg_time
                    
                    # send latency, processing time and average processing time to monitor
                    message = ("task_result", (worker_num,im_id, proc_time,avg_time,latency,result,time.time()))
                    p_message_queue.put(message)
                    if VERBOSE: print("w{}: Work processed image {} ".format(worker_num, im_id))
                    
                        
            # still update prev_time and count even if image wasn't processed
            prev_time = time.time()
            count += 1
            
        # no images in p_image_queue    
        except queue.Empty:
            time.sleep(0.1)
        
    print("w{}: Work thread exited. {} images processed".format(worker_num,count))

    
def write_data_csv(file,data,im_id,lock,num_validators=1):
    """
    Writes data in csv for, appending to file and labeling each row with im_id
    """
    lock.acquire()
    with open(file, mode = 'a') as f:
        for row in data:
            f.write((str(im_id) + "," + str(num_validators)))
            for val in row:
                f.write(",")
                f.write(str(val))
            f.write("\n") 
    lock.release()
     
def get_im_data(file,im_id,lock):
    try:
        lock.acquire()
        data = np.genfromtxt(str(file),delimiter = ',')
        lock.release()
    except OSError:
        print(file)
        return None,0
    # first row is im_id, second row is num_validators
    if np.size(data) > 0: # check whether there is any data
        out = data[np.where(data[:,0] == im_id)[0],:]
        if len(out) == 0: # check wether im_id is present
            return None, 0
        else:
            num_validators = out[0,1]
            return out, num_validators
    else:
        return None, 0
        
    
def update_data(file,num_validators, updated_data,lock):
    """
    Given one N x 10 arrray, replaces
    """
    lock.acquire()
    data = np.genfromtxt(file,delimiter = ',')
    im_id = updated_data[0,0]
    # update number of validators
    updated_data[:,1] = num_validators

    old_data_removed = data[np.where(data[:,0] != im_id)[0],:]
    new_data = np.concatenate((old_data_removed,updated_data),0)
    
    #overwrite data file
    with open(file, mode = 'w') as f:
        for row in new_data:
            f.write(str(row[0]))
            for val in row[1:]:
                f.write(" , ")
                f.write(str(val))
            f.write("\n") 
    lock.release()
    
