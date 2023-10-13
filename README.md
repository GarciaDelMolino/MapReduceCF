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
`python3 server.py --nmap 5 --nreduce 6 --casesensitive`


Any number of workers can be started running `python3 client.py` on different terminal sessions. Note that the number of concurrent workers is limited to max(N_map_tasks, N_reduce_tasks), i.e. 6 in the default setting. Any additional worker will be iddle while the others process tasks.


## How does this program work?

 If server is not available yet, client will try to re-connect after 1 second, with a max timeout of 1 minute. These parameters are also configurable from system settings.
  
```
export CONNECTION_TIMEOUT=60
export CONNECTION_RETRY=1
```
 Client will only exit when driver sends a disconnect signal.
 
 The worker (client) will send two kinds of signals to the driver (service): a connection request, and a finished message. The client will send two kinds of response: the task to be completed, or a kill signal. Only uppon reception of a kill signal will the client finish. 
 

## How to edit the communication protocol?

Edit `protos/mapreduce.proto` then execute `python -m grpc_tools.protoc -I protos --python_out=. --pyi_out=. --grpc_python_out=. .\protos\mapreduce.proto` to get compiled files `mapreduce_pb2.py` and `mapreduce_pb2_grpc.py`.

 
