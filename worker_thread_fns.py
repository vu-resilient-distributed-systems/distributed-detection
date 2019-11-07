# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:48:14 2019
@author: Derek Gloudemans

This file contains the functions executed by each of the threads in the worker
process
"""

import zmq
import random
import sys
import time
import os
import PIL
import queue
from PIL import Image
import argparse
import _pickle as pickle
import multiprocessing as mp
import threading
import numpy as np

def receive_images(p_image_queue,host = "127.0.0.1", port = 6200, timeout = 20, VERBOSE = True):
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
            im = pickle.loads(temp)
            p_image_queue.put(im) 
            prev_time = time.time()
            if VERBOSE: print("Image receiver thread received image at {}".format(time.ctime(prev_time)))
        except zmq.ZMQError:
            pass
        
    sock.close()
    if VERBOSE: print ("Image receiver thread closed socket.")


def send_messages(host,port,p_message_queue, timeout = 20, VERBOSE = True):
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
    while True:
        try:
            message = p_message_queue.get(timeout = timeout)
            payload = pickle.dumps(message)
            #sock.send_string(topic)
            sock.send_pyobj(payload)
            if VERBOSE: print("Sender thread sent message at {}".format(time.ctime(time.time())))
        
        except queue.Empty:
            break
    
    sock.close()
    context.term()
    if VERBOSE: print ("Message sender thread closed socket.")

def receive_messages(hosts,ports,out_queue, timeout = 20, VERBOSE = True):
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
            # parse topic and payload accordingly here
            data = pickle.loads(payload)
            out_queue.put(data)
            prev_time = time.time()
            if VERBOSE: print("Receiver thread received image at {}".format(time.ctime(prev_time)))
        
        except zmq.ZMQError:
            pass
        
    sock.close()
    context.term()
    if VERBOSE: print ("Message receiver thread closed socket.")

        
# tester code
if __name__ == "__main__":
    
    if False: #test receive_images
        p_queue = queue.Queue()
        receive_images(p_queue)
    
    if True: # test send and receive_images
        p_queue = queue.Queue()
        out_queue = mp.Queue()
        for i in range(10):
            payload = np.random.rand(10,10)
            item = ("label", payload)
            p_queue.put(item)
        
        t = threading.Thread(target = receive_messages, args = (["127.0.0.1"],[5200],out_queue,))    
        t.start()
        
        t2 = threading.Thread(target = send_messages, args = ("127.0.0.1",5200,p_queue,))
        t2.start()
        
        t.join()
        t2.join()
    
