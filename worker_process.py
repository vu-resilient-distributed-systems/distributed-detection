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

from worker_thread_fns import ( receive_images,send_messages,receive_messages,
                               load_balance,heartbeat,work_function, query_handler,
                               consistency_function, write_data_csv)


def worker(hosts,ports,audit_rate,worker_num, timeout = 20, VERBOSE = False,OVERWRITE = True):
    """
    Given a list of hosts and ports (one pair for each worker) and an id num,
    creates a series of threads and shared variables that carry out the work in
    a decentralized manner
    """
    
    # define shared variables
    p_image_queue = queue.Queue() # for storing received images
    p_new_id_queue = queue.Queue() # for storing received image names
    p_message_queue = queue.Queue() # for storing messages to send
    p_task_buffer = queue.Queue() # for storing tasks assigned to worker
    p_lb_results = queue.Queue() # for storing load balancing results
    p_audit_buffer = queue.Queue() # for storing audit requests
    p_query_requests = queue.Queue() # for storing query requests
    p_query_results = queue.Queue() # for storing query results
    p_consistency_results = queue.Queue() # for storing consistency query results
    
    # use multiprocessing Value for thread-shared values
    p_average_time = mp.Value('f',0.5+0.01*worker_num, lock = True)
    p_num_tasks = mp.Value('i',0,lock = True)
    p_last_balanced = mp.Value('i',-1,lock = True)
    
    p_database_lock = threading.Lock()
    p_continue_consistency = mp.Value('b',True,lock = True)
    
    # changeable worker parameters
    lb_timeout = 0.2
    query_timeout = 5
    consistency_rate = 0.5 # queries per second
    heart_rate = 0.05
    
    # get sender port
    host = hosts[worker_num]
    port = ports[worker_num]
    
    # offset port by 100 for UDP ports for query requests
    qr_port = ports[worker_num] + 100
    
    # get receiver ports
    hosts.remove(host)
    ports.remove(port)
    
    # overwrite datafile and write one dummy line
    if OVERWRITE:
        data_path = os.path.join('databases','worker_{}_database.csv'.format(worker_num))
        p_database_lock.acquire()
        with open(data_path,mode = 'w') as f:
            pass
        p_database_lock.release()
        write_data_csv(data_path,np.zeros([2,8])-1,-1,p_database_lock,-1)

    
    # create image receiver thread
    thread_im_rec = threading.Thread(target = receive_images, args = 
                                     (p_image_queue,
                                      p_new_id_queue,
                                      "127.0.0.1",
                                      6200,
                                      timeout,
                                      VERBOSE, 
                                      worker_num))
    
    # create message sender thread
    t_mes_send = threading.Thread(target = send_messages, args = 
                                     (host,
                                     port,
                                     p_message_queue,
                                     timeout,
                                     VERBOSE,
                                     worker_num))
    
    # create message receiver thread
    t_mes_recv = threading.Thread(target = receive_messages, args = 
                                    (hosts,
                                    ports,
                                    p_lb_results,
                                    p_audit_buffer,
                                    p_query_requests,
                                    p_query_results,
                                    p_consistency_results,
                                    timeout,
                                    VERBOSE,
                                    worker_num))
    
    # create load balancer thread
    t_load_bal = threading.Thread(target = load_balance, args = 
                                    (p_new_id_queue,
                                     p_task_buffer,
                                     p_lb_results,
                                     p_message_queue,
                                     p_average_time,
                                     p_num_tasks,
                                     p_last_balanced,
                                     audit_rate,
                                     timeout,
                                     lb_timeout, 
                                     VERBOSE,
                                     len(ports)+1, # total num workers
                                     worker_num))
    
    t_heartbeat = threading.Thread(target = heartbeat, args =
                                   (p_average_time,
                                    p_message_queue,
                                    p_num_tasks,
                                    p_continue_consistency,
                                    heart_rate,
                                    timeout,
                                    VERBOSE,
                                    worker_num))
    
    t_work = threading.Thread(target = work_function, args = 
                              (p_image_queue,
                              p_task_buffer,
                              p_audit_buffer,
                              p_message_queue,
                              p_average_time,
                              p_num_tasks,
                              p_last_balanced,
                              p_database_lock,
                              timeout, 
                              True,
                              worker_num))
    
    t_query = threading.Thread(target = query_handler, args = 
                               (host,
                                qr_port,
                                p_message_queue,
                                p_query_requests,
                                p_query_results,
                                p_database_lock,
                                query_timeout, 
                                timeout,
                                VERBOSE,
                                worker_num))
    
    t_consistency = threading.Thread(target = consistency_function, args = 
                                     (p_message_queue,
                                     p_consistency_results,
                                     p_last_balanced,
                                     p_database_lock,
                                     p_continue_consistency,
                                     consistency_rate, # queries per second
                                     query_timeout,
                                     True,
                                     worker_num))
                                    
    
    # start all threads
    thread_im_rec.start()
    t_mes_recv.start()
    t_mes_send.start()
    t_load_bal.start() 
    t_heartbeat.start()
    t_work.start()
    t_query.start()
    t_consistency.start()
    
    # wait for all threads to terminate
    thread_im_rec.join()
    t_mes_recv.join()
    t_load_bal.join()  
    t_work.join()
    t_mes_send.join()
    t_heartbeat.join()
    t_query.join()
    t_consistency.join()
    
