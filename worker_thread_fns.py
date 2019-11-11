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
p_average_time = 0 # this is necessary per global variable stuff

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
    
    if VERBOSE: print ("Image receiver thread connected to socket.")
    
    # main receiving loop
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            temp = sock.recv_pyobj(zmq.NOBLOCK)
            (name,im) = pickle.loads(temp)
            p_image_queue.put(im) 
            p_new_im_id_queue.put(name)
            prev_time = time.time()
            if VERBOSE: print("Image receiver thread {} received image {} at {}".format(num,name,time.ctime(prev_time)))
        except zmq.ZMQError:
            time.sleep(0.1)
            pass
        
    sock.close()
    if VERBOSE: print ("Image receiver thread {} closed socket.".format(num))


def send_messages(host,port,p_message_queue, timeout = 20, VERBOSE = True,num = 0):
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
    time.sleep(10) # pause to allow subscribers to connect
    
    # sending loop
    prev_time = time.time()
    while time.time() - prev_time < timeout:
        try:
            message = p_message_queue.get(timeout = timeout)
            payload = pickle.dumps(message)
            #sock.send_string(topic)
            sock.send_pyobj(payload)
            if VERBOSE: print("Sender thread {} sent message at {}".format(num,time.ctime(time.time())))
            prev_time = time.time()
            
        except queue.Empty:
            time.sleep(0.1)
            break
    
    sock.close()
    context.term()
    if VERBOSE: print ("Message sender thread {} closed socket.".format(num))

def receive_messages(hosts,ports,p_lb_queue, timeout = 20, VERBOSE = True,num = 0):
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
            if VERBOSE: print("Receiver thread {} received message at {}".format(num,time.ctime(prev_time)))
            # parse topic and payload accordingly here
            (label,data) = pickle.loads(payload)
            
            # deal with different types of messages (different label field)
            if label == "heartbeat":
                 # (time heartbeat generated, this workers wait time, worker_num)
                p_lb_queue.put((data[0],data[1]))
            else:
                pass

        
        except zmq.ZMQError:
            time.sleep(0.1)
            pass
        
    sock.close()
    context.term()
    if VERBOSE: print ("Message receiver thread {} closed socket.".format(num))


def heartbeat(p_message_queue,p_task_queue,interval = 0.5,timeout = 20, VERBOSE = True, num = 0):
    """
    Sends out as a heartbeat the average wait time at regular intervals
    p_average_time - shared variable for average time,
    """
    global p_average_time

    last_average_time = p_average_time
    prev_time = time.time()
    
    while prev_time + timeout > time.time():
        message = ("heartbeat",(time.time(),p_average_time* p_task_queue.qsize() ,num))
        p_message_queue.put(message)
        if VERBOSE: print("Heartbeat thread {} added heartbeat to message queue.".format(num))
        
        # updates prev_time whenever p_average_time changes
        if p_average_time != last_average_time:
            prev_time = time.time() 
        
        # a bit imprecise since the other operations in the loop do take some time
        time.sleep(interval)
    
    if VERBOSE: print("Heartbeat thread {} exited.".format(num))
    


def load_balance(p_new_image_id_queue,p_task_queue,p_lb_results,p_message_queue, timeout = 20, lb_timeout = 2, VERBOSE = True,num = 0):
    """
    Every time a new image is added to the new_image_id_queue, sends worker's 
    current estimated wait time to all other workers, waits for timeout, and 
    compares current estimated wait times to those of all workers received after
    the message was sent. Adds image to task list if worker has min wait time
    """
    global p_average_time
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
    
            # this worker has minimum time to process, so add to task queue
            if min_time > p_task_queue.qsize() * p_average_time:
                p_task_queue.put(im_id)
                if VERBOSE: print("Worker {} Added {} to task list.".format(num,im_id))
         
        # there are no items in new_image_id_queue
        except queue.Empty:
            time.sleep(0.1)
            pass
                
    if VERBOSE: print("Load balancer thread {} exited.".format(num))
     
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
        p_task_queue = queue.Queue()
        p_lb_results = queue.Queue()
        p_average_time = random.random()
        
        t_im_recv = threading.Thread(target = receive_images, args = (p_image_queue,p_new_id_queue,))
        t_load_bal = threading.Thread(target = load_balance, 
                                      args = (p_new_id_queue,p_task_queue,
                                              p_lb_results,p_message_queue,))
        t_send_messages = threading.Thread(target = send_messages, args = ("127.0.0.1",5200,p_message_queue,))
        t_heartbeat = threading.Thread(target = heartbeat, args = (p_message_queue,p_task_queue,))
        
        t_heartbeat.start()
        t_im_recv.start()
        t_load_bal.start()
        t_send_messages.start()
        
        
        t_im_recv.join()
        t_load_bal.join()
        t_send_messages.join()
        t_heartbeat.join
        

