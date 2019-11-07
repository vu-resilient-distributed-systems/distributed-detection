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
from PIL import Image
import argparse
import _pickle as pickle
import multiprocessing as mp

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
    
        
# tester code
if __name__ == "__main__":
    
    if True: #test receive_images
        queue = mp.Queue()
        receive_images(queue)
    
