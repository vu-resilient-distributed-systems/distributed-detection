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

from worker_process import worker
    
if __name__ == "__main__":
    # tester code for the worker processes
    
    hosts = []
    ports = []
    num_workers = 4
    timeout = 30
    VERBOSE = False
    audit_rate = mp.Value('f',0.1,lock = True)
    
    for i in range(num_workers):
        hosts.append("127.0.0.1")
        ports.append(5200+i)
    
    processes = []
    for i in range(num_workers):
        p = mp.Process(target = worker, args = (hosts,ports,audit_rate,i,timeout,VERBOSE,))
        p.start()
        processes.append(p)
    print("All processes started")
    
    for p in processes:
        p.join()
    print("All processes terminated")    
    