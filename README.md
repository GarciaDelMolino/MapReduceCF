# MapReduce to Count Word Occurences

This repo contains all methods and helpers to run a distributed map-reduce program.

## Requirements:

 - Python 3.7 or higher
 - pip version 9.0.1 or higher
 - gRPC

```
python -m pip install --upgrade pip
python -m pip install grpcio
```

## How to run this repo:

In one terminal, start the driver calling `python3 server.py`. By default, 6 map tasks will be set, for 4 reduce operations. The word count will not be case-sensitive. 

Those parameters can be changed from system settings 
```
export CASE_SENSITIVE=0
export N_MAP=6
export N_REDUCE=4
```

or given as arguments to the server call:
`python3 server.py --n_map 5 --n_reduce 6 --case_sensitive`


Any number of workers can be started running `python3 client.py` on different terminal sessions. Note that the number of concurrent workers is limited to N_map_tasks and N_reduce_tasks. Any additional worker will be iddle while the others process tasks.

The program is able to handle any number of N_map_tasks and N_reduce_tasks, even in cases larger than the text file population or alphabetic letters.


## How does this program work?

 If server is not available yet, client will try to re-connect after 1 second, with a max timeout of 1 minute. These parameters are also configurable from system settings.
  
```
export CONNECTION_TIMEOUT=60
export CONNECTION_RETRY=1
```
 Client will only exit when driver sends a disconnect (KILL) signal.
 
 The worker (client) will communicate with the driver (service) when it is ready to complete a task. This can happen when the client starts but also when the client has completed the requested task. The server will send a response with information on the task to be completed. The task can be MAP, REDUCE, WAIT or KILL. Only uppon reception of a kill signal will the client exit. 
 
 The server will make workers WAIT for all MAP tasks to finish before sending REDUCE tasks. Only when all REDUCE tasks have finish will the server send KILL instructions. The server will wait for all waiting workers to request a new task (KILL) and will self-shutdown when no more workers are waiting for tasks.
 

## Maintenance

### Things to improve

- We have no way of tracking that the client did not fail/stop unexpectedly
- For code simplicity, workers might be asked to wait unnecessary after completion of a task.
- Word-count: how to split grammar abreviations? e.g. "he's" instead of "he is" or "Andrew's", etc

### How to edit the communication protocol?

gRPC communication structs and methods are defined in a proto file. 

To add params into the communication messages between clients and server, or to add new methods, edit `protos/mapreduce.proto`. To compile the changes, execute `python -m grpc_tools.protoc -I protos --python_out=. --pyi_out=. --grpc_python_out=. .\protos\mapreduce.proto`. This will produce new compiled files `mapreduce_pb2.py` and `mapreduce_pb2_grpc.py`, which are used by our server and client. 

New methods must be implemented in `class Driver(mapreduce_pb2_grpc.DriverServicer)`.

### Testing

You can define unitest cases using script `create_unitest_files.py`.  The script will generate `n_files` in folder `input_path`, containing all words in `word_count`.

The client is able to handle cases in which there are less files than map tasks, as well as cases in which there are less first letters than reduce tasks.
 