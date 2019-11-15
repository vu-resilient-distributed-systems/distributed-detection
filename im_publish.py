# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 18:48:14 2019
@author: Derek Gloudemans

This file publishes images via ZMQ socket at regular intervals, 
continuing indefinitely 
"""
# cd "C:\Users\derek\OneDrive\Documents\Derek's stuff\Not Not School\Lab\Code\distributed-detection
# python im_publish.py 1 "C:\Users\derek\Desktop\KITTI\Tracking\Tracks\training\image_02\0000"

import zmq
import random
import sys
import time
import os
import PIL
import cv2
from PIL import Image
import argparse
import _pickle as pickle
import numpy as np


def publish_images(imlist,tpi,socket):
    """ publish an image from imlist via given socket at given time between images
    imlist - list of strings - each string is the full path to an image file
    tpi - time per image sent
    socket - ZMQ publisher socket bound to a port
    """
    
    topic = "images"
    i = 0
    prev_time = time.time()-1
    
    while True:
        #im = open(imlist[i%len(imlist)],'rb')
        im = Image.open(imlist[i%len(imlist)]).convert('RGB')
        # convert to cv_im
        cv_im = np.array(im)
        cv_im = cv_im[:,:,::-1].copy()
        

#        im = np.random.rand(10,10)
        message = (i,cv_im)
        im_pickle = pickle.dumps(message)
#        open_cv_image = np.array(pil_im) 
#        # Convert RGB to BGR 
#        return open_cv_image[:, :, ::-1]
        
        # wait until it's time to send the image again
        while time.time() - prev_time < tpi:
            pass
        
        prev_time = time.time()
        socket.send_pyobj(im_pickle)
        print("Sent image {}".format(i))
        i = i + 1
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Get input directory and publication rate.')
    parser.add_argument("time_per_image",help='<Required> float',type = float)
    parser.add_argument("im_directory",help='<Required> string',type = str)
    args = parser.parse_args()
    
    # parse args
    time_per_im =  args.time_per_image
    PATH = args.im_directory
    
    port = 6200
    host = "127.0.0.1" # Host IP address
    
    context = zmq.Context()
    sock = context.socket(zmq.PUB)
    sock.bind("tcp://{}:{}".format(host, port))
    #sock.bind(base_address+":"+str(port))
    
    imlist = [os.path.join(PATH,file) for file in os.listdir(PATH)]
    
    publish_images(imlist,time_per_im,sock)