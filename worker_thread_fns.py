# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:48:14 2019
@author: Derek Gloudemans

This file contains the functions executed by each of the threads in the worker
process
"""

import zmq
import random
import time
import queue
from PIL import Image
import _pickle as pickle
import multiprocessing as mp
import threading
import numpy as np

from simple_yolo.yolo_detector import Darknet_Detector

def receive_images(p_image_queue,p_new_im_id_queue, host = "127.0.0.1", port = 6200, timeout = 20, VERBOSE = True,num = 0):
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
    
    print ("w{}: Image receiver thread connected to socket.".format(num))
    
    # main receiving loop
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            temp = sock.recv_pyobj(zmq.NOBLOCK)
            (name,im) = pickle.loads(temp)
            p_image_queue.put((name,im,time.time())) 
            p_new_im_id_queue.put(name)
            prev_time = time.time()
            if VERBOSE: print("w{}: Image receiver thread received image {} at {}".format(num,name,time.ctime(prev_time)))
        except zmq.ZMQError:
            time.sleep(0.1)
            pass
        
    sock.close()
    print ("w{}: Image receiver thread closed socket.".format(num))


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


def receive_messages(hosts,ports,p_lb_queue, p_audit_buffer, timeout = 20, VERBOSE = False,worker_num = 0):
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
                 # (time heartbeat generated, this workers wait time, worker_num)
                p_lb_queue.put((data[0],data[1]))
            
            # deal with audit requests
            elif label == "audit_request":
                # send audit_requests to two other 
                (im_id,auditors) = data
                if worker_num in auditors:
                    p_audit_buffer.put(im_id)
            else:
                pass

        
        except zmq.ZMQError:
            time.sleep(0.1)
            pass
        
    sock.close()
    context.term()
    print ("w{}: Message receiver thread closed socket.".format(worker_num))


def heartbeat(p_average_time,p_message_queue,p_num_tasks,interval = 0.5,timeout = 20, VERBOSE = False, worker_num = 0):
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
    
    while prev_time + timeout > time.time():
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
    
    print("w{}: Heartbeat thread exited.".format(worker_num))
    


def load_balance(p_new_image_id_queue,p_task_buffer,p_lb_results,p_message_queue,
                 p_average_time,p_num_tasks, timeout = 20, lb_timeout = 2, 
                 VERBOSE = True,num = 0):
    """
    Every time a new image is added to the new_image_id_queue, sends worker's 
    current estimated wait time to all other workers, waits for timeout, and 
    compares current estimated wait times to those of all workers received after
    the message was sent. Adds image to task list if worker has min wait time
    """
    prev_time = time.time()
    all_received_times = []
    
    while time.time() - prev_time < timeout:
        # if there is a new image id
        try:
            # grabs new image id from queue if there is one
            im_id = p_new_image_id_queue.get(timeout = 0)
            time_received = time.time()
            
            # get all pending times in p_lb_results and append to all_received_times
            while True:
                try: 
                    message = p_lb_results.get(timeout = 0)
                    all_received_times.append(message)
                except queue.Empty:
                    break

            # go through all_received times and remove all messages more than lb_timeout old
            # also keep running track of min valid time
            min_time = np.inf
            deletions = []
            for i in range(len(all_received_times)): 
                # each item is of form (time_generated, wait time, worker_num)
                (other_gen_time, other_wait_time) = all_received_times[i]
                
                # check if item is out of date
                if other_gen_time + lb_timeout < time_received:
                    deletions.append(i)
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
            
            # this worker has minimum time to process, so add to task queue
            if min_time >= cur_wait:
                p_task_buffer.put(im_id)
                if VERBOSE: print("w{}: Load balancer added {} to task list.".format(num,im_id))
                
                
                
            prev_time = time_received
            
        # there are no items in new_image_id_queue
        except queue.Empty:
            time.sleep(0.1)
            pass
                
    print("w{}: Load balancer thread exited.".format(num))
 
def work_function(p_image_queue,p_task_buffer,p_audit_buffer,p_message_queue,audit_rate,
                  p_average_time,timeout = 20, VERBOSE = True, worker_num = 0,num_workers = 1):
    """
    -Repeatedly gets first image from image_queue. If in audit_buffer or task_buffer,
    processes image. Otherwise, discards image and loads next.
    -Processing an image consists of performing object detection on the image and
    outputting the results (numpy array)
    -If not audit, the results are written to a local data file, and processing speed, latency, and average processing speed are reported to monitor process
    -If audit, full results are sent to monitor process
    """ 
    
    # create network for doing work
    model = Darknet_Detector('simple_yolo/cfg/yolov3.cfg',
                                'simple_yolo/yolov3.weights',
                                'simple_yolo/data/coco.names',
                                'simple_yolo/pallete')
    audit_list = [] # will store all im_ids taken from p_audit_buffer
    task_list = [] # will store all im_ids taken from p_task_buffer
    
    time.sleep(5)
    count = 0
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            # get image off of image_queue
            (im_id,image,im_time_received) = p_image_queue.get(timeout = 0)
            
            
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
            if im_id in audit_list:
                AUDIT = True
                
            if AUDIT or TASK:
                if VERBOSE: print("w{} work began processing image {}.".format(worker_num, im_id))
                count += 1
                # do work
                work_start_time = time.time()
                #result, _ = model.detect(image)
                result = np.zeros([10,10])
                work_end_time = time.time()
                prev_time = time.time()
                
                # if audit, write results to monitor process
                if AUDIT:
                    # package message
                    message = ("audit_result",(im_id,worker_num,result))
                    p_message_queue.put(message)
                    if VERBOSE: print("w{}: work audit results on image {} to message queue".format(worker_num, im_id))
                # if task, write results to database and report metrics to monitor process
                if TASK:
                    # write results to database
                    #TODO!!!
                    
                    # compute metrics
                    latency = work_end_time - im_time_received
                    proc_time = work_end_time - work_start_time
                    
                    # update average time with weighting of current speed at 0.2
                    with p_average_time.get_lock():
                        avg_time = p_average_time.value*0.8 + 0.2* proc_time
                        p_average_time.value = avg_time
                    
                    # send latency, processing time and average processing time to monitor
                    message = ("metrics", (worker_num, proc_time,avg_time,latency))
                    p_message_queue.put(message)
                    if VERBOSE: print("w{}: Work metrics on image {} sent to message queue".format(worker_num, im_id))
                    
                    # if this task is not itself an audit, randomly audit 
                    #with p_audit_rate probability
                    with audit_rate.get_lock():
                        audit_rate_val = audit_rate.value
                    
                    
                    if not AUDIT and random.random() < audit_rate_val:
                        # send result to monitor process
                        message = ("audit_result",(im_id,worker_num,result))
                        p_message_queue.put(message)
                        
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
                        if VERBOSE: print("w{}: Work requested audit of image {}".format(worker_num, im_id))
                        
            else:
                # still update prev_time if image was not a task or an audit
                prev_time = time.time()
        # no images in p_image_queue    
        except queue.Empty:
            time.sleep(0.1)
        
    print("w{}: Work thread exited. {} images processed".format(worker_num,count))

    
    
    
    
    
    
# tester code
if __name__ == "__main__":
    
    # Test 1 - Ensure receive images works
    if False: #test receive_images
        p_queue = queue.Queue()
        p_name_queue = queue.Queue()
        receive_images(p_queue,p_name_queue)
    
    # Test 2- Test Send and receive messages functions
    if False: 
        p_queue = queue.Queue()
        out_queue = mp.Queue()
        for i in range(10):
            payload = np.random.rand(10,10)
            item = ("label", payload)
            p_queue.put(item)
        
        t = threading.Thread(target = receive_messages, args = (["127.0.0.1","127.0.0.1"],[5200,5201],out_queue,))    
        t.start()
        
        t2 = threading.Thread(target = send_messages, args = ("127.0.0.1",5200,p_queue,))
        t2.start()
        
        
        t.join()
        t2.join()

    # Test 3 - Test load balancer works in case when no messages are received
    if False:
        """
        set up a test in which images are received and added to new image queue
        load_balancer should add messages to send queue and sender should send them
        load_balancer should time out without receiving any responses,
        and thus add the image id to its task list
        """
        
        # define shared variables
        p_image_queue = queue.Queue()
        p_new_id_queue = queue.Queue()
        p_message_queue = queue.Queue()
        p_task_buffer = queue.Queue()
        p_lb_results = queue.Queue()
        p_average_time = mp.Value('f',1+0.01*worker_num, lock = True)
        p_num_tasks = mp.Value('i',0,lock = True)
        
        t_im_recv = threading.Thread(target = receive_images, args = (p_image_queue,p_new_id_queue,))
        t_load_bal = threading.Thread(target = load_balance, 
                                      args = (p_new_id_queue,p_task_buffer,
                                              p_lb_results,p_message_queue,p_average_time,p_num_tasks))
        t_send_messages = threading.Thread(target = send_messages, args = ("127.0.0.1",5200,p_message_queue,))
        t_heartbeat = threading.Thread(target = heartbeat, args = (p_average_time,p_message_queue,p_num_tasks))
        
        t_heartbeat.start()
        t_im_recv.start()
        t_load_bal.start()
        t_send_messages.start()
        
        
        t_im_recv.join()
        t_load_bal.join()
        t_send_messages.join()
        t_heartbeat.join()
        
    # Test 4 - Test wrk_function
    if True:
        """
        set up a test in which images are received and added to new image queue
        load_balancer should add messages to send queue and sender should send them
        load_balancer should time out without receiving any responses,
        and thus add the image id to its task list
        """
        
        # define shared variables
        p_image_queue = queue.Queue()
        p_new_id_queue = queue.Queue()
        p_message_queue = queue.Queue()
        p_task_buffer = queue.Queue()
        p_lb_results = queue.Queue()
        p_audit_buffer = queue.Queue()
        
        p_average_time = mp.Value('f',1+0.01*0, lock = True)
        p_num_tasks = mp.Value('i',0,lock = True)
        
        audit_rate = mp.Value('f',0.1,lock = True)
        
        t_im_recv = threading.Thread(target = receive_images, args = (p_image_queue,p_new_id_queue,))
        t_load_bal = threading.Thread(target = load_balance, 
                                      args = (p_new_id_queue,p_task_buffer,
                                              p_lb_results,p_message_queue,p_average_time,p_num_tasks))
        t_send_messages = threading.Thread(target = send_messages, args = ("127.0.0.1",5200,p_message_queue,))
        t_heartbeat = threading.Thread(target = heartbeat, args = (p_average_time,p_message_queue,p_num_tasks,))
        t_work = threading.Thread(target = work_function, 
                                  args = (p_image_queue,p_task_buffer,p_audit_buffer,p_message_queue,audit_rate,p_average_time,
                                          20, True, 0,2))
        
        
        t_heartbeat.start()
        t_im_recv.start()
        t_load_bal.start()
        t_send_messages.start()
        t_work.start()
        
        t_im_recv.join()
        t_load_bal.join()
        t_send_messages.join()
        t_heartbeat.join()
        t_work.join()