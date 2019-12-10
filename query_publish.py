# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:48:14 2019
@author: Derek Gloudemans

This file publishes images via ZMQ socket at regular intervals, 
continuing indefinitely 
"""
# cd "C:\Users\derek\OneDrive\Documents\Derek's stuff\Not Not School\Lab\Code\distributed-detection
# python query_publish.py 1 4

import zmq
import socket
import random
import time
from PIL import Image
import argparse
import _pickle as pickle
import numpy as np

from matplotlib import style
import matplotlib.pyplot as plt

def publish_queries(rate,pub_socket,im_sub_socket,output_sub_socket,worker_hosts,worker_ports,VERBOSE = True):
    """ publishes a request for information on a given image at a random rate. Each
    request is randomly sent to one worker. At the end, plots the active and completed
    queries over the time of the trial
    
    rate - float -  average rate of queries (0.5) = 0.5 queries per second
    socket - socket to publish queries on 
    
    imlist - list of strings - each string is the full path to an image file
    tpi - time per image sent
    pub_socket - a UDP socket bound to a port for publishing queries
    im_sub_socket - ZMQ subscriber socket bound to a port for receiving images
    worker_hosts - list of strings - IP addresses for each worker
    worker_ports - list of ints - port for each worker
    """
    #for plotting
    START_TIME = time.time()
    x = []
    y = []
    y2 = []
    
    all_active_queries = [] 
    completed_queries = []
    all_im_ids = []
    next_time = time.time() + np.random.normal(1/rate,3)
    
    last_im_time = time.time()
    last_active_time = time.time()
    
    # time out when queries are no longer being answered
    while last_active_time + 20 > time.time() :
        try:
            # receive an im_id and add it to list of valid im ids
            temp = im_sub_socket.recv_pyobj(zmq.NOBLOCK)
            (name,im) = pickle.loads(temp)
            all_im_ids.append(name)
            last_im_time = time.time()
        except zmq.ZMQError:
            pass
        
        try:
            # receive an im_id and add it to list of valid im ids
            temp = output_sub_socket.recv_pyobj(zmq.NOBLOCK)
            (label,data) = pickle.loads(temp)
            if label == "query_output":
                # data is numpy array
                if VERBOSE:
                    if type(data[1]) == np.ndarray: 
                        print("{} objects detected in image {}.".format(len(data[1]),data[0]))
                        
                        # indicate that query has been answered
                        if data[0] in all_active_queries:
                            all_active_queries.remove(data[0])
                            completed_queries.append(data[0])
                            last_active_time = time.time()
                        
                    else:
                        print("No results yet for image {}.".format(data[0]))
        except zmq.ZMQError:
            pass
        
        
        # send a new query if sufficient time has passed
        if time.time() > next_time and len(all_im_ids) > 0 :
            
            # repeat an active query
            if len(all_active_queries) > 0:
                random.shuffle(all_active_queries)
                repeat_im_id = all_active_queries[0]
                idx = random.randint(0,len(worker_ports)-1)
                worker_addr = worker_hosts[idx]
                worker_port = worker_ports[idx]
                message = str(repeat_im_id)
                pub_socket.sendto(message.encode('utf-8'),(worker_addr,worker_port))
            
            # send  new query as long as new images are being received
            if last_im_time + 10 > time.time():
                im_id = -1
                tries = 0
                # randomly try to obtain an image ID that hasn't yet been queried
                while tries < 5 and (im_id == -1 or im_id in all_active_queries or im_id in completed_queries):
                    random.shuffle(all_im_ids)
                    im_id = all_im_ids[0]
                    all_im_ids.remove(im_id)
                    tries += 1
                all_active_queries.append(im_id)
            
            # get random worker
            idx = random.randint(0,len(worker_ports)-1)
            worker_addr = worker_hosts[idx]
            worker_port = worker_ports[idx]
            
            # combine into single string
            message = str(im_id)
            pub_socket.sendto(message.encode('utf-8'),(worker_addr,worker_port))
            
            if VERBOSE:
                print("Sent query request: " + message)
            next_time = time.time() + np.random.normal(1/rate,3)
            
            # append results 
            print("{} active queries remaining.".format(len(all_active_queries)))
            x.append(time.time()-START_TIME)
            y.append(len(all_active_queries))
            y2.append(len(completed_queries))
            
    # close sockets        
    pub_socket.close()
    im_sub_socket.close()
    
    # finally, plot results
    style.use('fivethirtyeight')
    plt.figure()
    plt.stackplot(x,y,y2)
    plt.xlabel("Time (s)")
    plt.ylabel("Queries")
    plt.title("Queries completed during trial.")
    plt.legend(['Active queries','Completed queries'])
    plt.show()
    
    print ("Closed im receiver and query sender sockets.")
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Get input directory and publication rate.')
    parser.add_argument("rate",help='<Required> float',type = float)
    parser.add_argument("num_workers",help='<Required> int',type = int)
    args = parser.parse_args()
    
    # parse args
    rate =  args.rate
    num_workers = args.num_workers
    
    # create socket to receive images
    im_port = 6200
    im_host = "127.0.0.1" # Host IP address
    context = zmq.Context()
    im_sock = context.socket(zmq.SUB)
    im_sock.connect("tcp://{}:{}".format(im_host,im_port))
    im_sock.subscribe(b'') # subscribe to all topics on this port (only images)
    
    # create socket to listen for query outputs
    output_context = zmq.Context()
    out_sock = output_context.socket(zmq.SUB)
    for i in range(num_workers):
        out_sock.connect("tcp://{}:{}".format("127.0.0.1",5200 + i))
    out_sock.subscribe(b'') # subscribe to all topics on this port (only images)
    
    # get list of all worker ports
    worker_hosts = []
    worker_ports = []
    for i in range(num_workers):
        worker_hosts.append("127.0.0.1")
        worker_ports.append(5300+i)
    print("Opened image subscriber socket")
    
    # create UDP socket to send queries
    qr_port = 6300
    qr_host = "127.0.0.1"
    qr_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print("Opened query publisher socket")
    
    publish_queries(rate,qr_sock,im_sock,out_sock,worker_hosts,worker_ports)