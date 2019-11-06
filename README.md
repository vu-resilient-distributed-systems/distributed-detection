# Distributed Detection

This repository is a final project for my Resilient Distributed Systems class. A multi-worker image processing system is implemented. Images are received at regular intervals from one or multiple sources and worker processes detect objects in these images. Workers distribute computational load in a decentralized manner, and data is stored in a decentralized manner as well. The database is structured for availability, functionality when partitioned, and eventual consistence (assuming no malicious workers). A simple monitoring process is also used for basic fault detection and management. The following requirements will be met:
- System functions as long as one worker process is active (high availability)
- Database querying is done using all currently active database nodes (partition-tolerant)
- Data is stored as (incomplete) replicas in each process and is iteratively checked against majority of worker processes and adjusted to the majority value (eventual consistency)
- Audit system used to check processes occasionally against 2 other processes
- Component process health monitor detects and restarts faulty processes

## Design Specification
### 1. System Actors
The main actors in the system are the worker processes which perform object detection on incoming images and store results. The monitoring process is an additional actor. While the image-sender and database query clients are involved in the operation of the system, they are considered separate to the system (i.e. the system is not designed to be robust to their failure and it is assumed that they do not deviate from expected operating behavior.
![](readme_ims/diagram0.jpg)

### 2. Summary of System Workflow
Each worker process is designed to operate more or less independently from each other worker process. While there is communication between workers to coordinate load balancing, majority responses to query requests, results audits and database consistency, all of these actions are performed with a fixed time timeout such that a partitioned system can remain available and is not deadlocked waiting for responses. Note that in the case when no workers respond within the fixed timeout, the worker process performs as a single agent would. The main components of each worker process are explained below:
#### Worker Process Shared Objects
- image_queue - holds all images that are waiting to be processed
- task_list - stores ids of all images that the worker process is assigned to process
- new_image_id_queue - stores ids of all images added to the image_queue
- messages_to_send_queue - all threads write messages to this queue (destined for other workers) and sender_thread sends them
- load_balancing_queue - all information received from other workers necessary for load balancing is written to this queue
- audit_request_queue - whenever an audit is requested from a worker, the image id and auditee process id are appended to the audit request queue
- audit_result_queue - results from audits are written here
- query_request_queue - all queries (either by outside client or another process) are stored here
- query_result_queue - all query results are stored here
- average_processing_speed - used to make load balancing estimations
#### Worker Process Threads
- Image Receiver Thread - on a dedicated socket, listens for images and writes them to image_queue whenever received
- Load Balancing Thead - whenever a new image id is added to new_image_id_queue, load balancing thread computes the worker's estimated wait time and sends it to all other workers (appends this message to the messages_to_send_queue). After a fixed timeout period, if the worker's estimated time is the minimum of all estimated wait times computed, the worker appends the image id to the task_list.
- Message Sender Thread - whenever the messages_to_send_queue is not empty, sends the message in the queue to other worker processes
- Message Receiver Thread - parses all received messages from other worker processes and appends them to the appropriate queue
- Heartbeat Thread - periodically sends a heartbeat to the Monitor Process.
- **Work Thread** - if there is an image in the audit queue, it is selected, otherwise, an image is selected from the task_list. Performs object detections on the image, and if selected by random probability for audit, sends this result to 2 random other processes. Waits for two PASS messages on the audit_result_queue. Conversely, if conducting an audit, the result is checked against the result of the worker process being audited and PASS or FAIL message is sent back to that worker. After detection and auditing, the result is written to local worker process database. Statistics such as average processing speed, latency are sent to the monitor process).
- Query Handling Thread - grabs a query from the query request queue. If from an external client, forwards this request to all other workers. If from another worker, sends a message with that worker's result for the queried value. After a timeout period, the query handling thread of the first worker process returns the majority vote value to the external client. It then updates its database with the majority value and the number of corroborating workers only if the number of corroborating workers is greater than the number of corroborating workers for the existing value stored in the worker's database. In this way, eventual consistency is obtained for an entry by majority vote whenever there is an external client query.
- Consistency Thread - since the client query updates are possibly random, the consistency thread creates additional, interal queries working backwards in time to the other workers and uses these values to update its database entries in the same way. This makes eventual consistency much faster.

The rough process flow diagram for one worker process shown below.
![](readme_ims/diagram1.jpg)

Data is stored in JSON or similar files. To keep things simple, the tester script will evaluate the system on a single machine in multiple processes. The neural network detection will thus be carried out on GPU by one process, and cpu by other processes, creating an interesting disparity in processing speed.

### 3. External Libraries and Services Required
Multiprocessing and multithreading modules will be used, as well as ZMQ middleware for message passing. Additionally, pytorch will be used for object detection neural network.

### 4. System Failure Modes and Anomaly Detectors
The following failures are possible and will be tested by artificial introduction. The anomaly detection method is listed in parentheses:
- Worker returns anomalous values (audit result flag sent to system monitor)
- Worker becomes unresponsive or dies (heartbeat not receieved by monitor)
- System becomes partitioned temporarily (i.e. some nodes are restarted, or some nodes are temporarily isolated from communication so that messages are lost) (query handling thread reports low number of corroborating nodes)
- Audit ratio is too high and creates low throughput (audit queue length is high much of the time, latency is high)
- Over time, all original processes are killed (i.e. will the data consistency strategy be sufficient to ensure data is not lost)
- All workers fail at once 

The following failures are not accounted for or are considered outside the scope of the system:
- Monitor process fails or is itself anomalous
- Image sender stops sending images
- External clients stop querying system

### 5. Failure Mitigation Strategies
- Worker returns anomalous values - system monitor will maintain a count of failed audits by a process. If a set number of audit failures are received, the process will be restarted.
- Worker becomes unresponsive - after a set time without receiving a heartbeat from a worker, the system monitor will restart that worker process
- System becomes partitioned temporarily - the system should be robust to partitioning
- Audit ratio is too high - if latency and audit queue lengths become too high, audit ratio will be adjusted (conversely, it will slowly increase over time if latency remains low)
- Over time, all original processes are killed - the database should be robust to this type of rolling failure, other than that images in the task list of the worker process when it was killed will not be processed. This is assumed to be an acceptable level of failure since data is assumed to be received in a realtime manner where long term storage of raw image data to mitigate such failures is too resource intensive.
- All workers fail at once - this should likewise be handled gracefully other than that 


