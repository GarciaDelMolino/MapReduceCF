from __future__ import print_function
from time import sleep, time
import os
import logging
import json

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc


log_fn = print  # logging.info

def map_file(metadata):
    """
    Reads words in file and writes into the relevant bucket.

    :param metadata: dct containing files to read, task ID and number of buckets.
    :return: None
    """
    files = metadata['files'].split(',')
    task = metadata['taskID']
    M = metadata['buckets']
    log_fn(f"MAP{task}: Extract words from {files} and write into {M} buckets")
    sleep(10)
    return None


def reduce(metadata):
    """
    Reads words from buckets and writes count in output file.

    :param metadata: dct containing files to read, taskID and case sensitive flag.
    :return: None
    """

    files = metadata['files'].split(',')
    task = metadata['taskID']
    cased = metadata['case_sensitive']
    log_fn(f"REDUCE{task}: Count words from {files} with case sensitive {cased}")
    sleep(10)
    return None

def run():
    """
    Client to process tasks as instructed by the driver.
    The tasks can be MAP, REDUCE, WAIT or KILL
    The client will wait for the driver to be online up to a timeout.
    It will only exit in case of timeout (server is offline) or upon reception of the KILL instruction.

    :return: None
    """
    # client will only close when the driver instructs so.
    log_fn("Start client. Will try to connect ...")
    timeout = os.environ.get("CONNECTION_TIMEOUT", 60)
    retry_time = os.environ.get("CONNECTION_RETRY", 1)
    t0 = time()
    channel = grpc.insecure_channel("localhost:50051")
    stub = mapreduce_pb2_grpc.DriverStub(channel)
    task_req = mapreduce_pb2.TaskRequest(status='new')
    while True:
        # client waits for a task until KILL signal is received.
        try:
            response = stub.GetTask(task_req)
        except grpc._channel._InactiveRpcError:
            # there's probably a better way to handle this timeout using grpc kwargs
            log_fn("Waiting for server connection")
            sleep(retry_time)
            if (time()-t0) > timeout:
                logging.warning("Could not establish connection. Exiting.")
                break
            continue

        print(f"Task received: {response.task} {response.metadata}")
        # do stuff according to task
        if response.task == 'KILL':
            log_fn("Nothing to be done. Exiting.")
            break
        elif response.task == 'MAP':
            metadata = json.loads(response.metadata)
            map_file(metadata)
            task_req = mapreduce_pb2.TaskRequest(status=f'done MAP{metadata.get("taskID", -1)}')
        elif response.task == 'REDUCE':
            metadata = json.loads(response.metadata)
            reduce(metadata)
            task_req = mapreduce_pb2.TaskRequest(status=f'done REDUCE{metadata.get("taskID", -1)}')
        elif response.task == 'WAIT':
            log_fn("Waiting for a task to be available.")
            sleep(retry_time)
            task_req = mapreduce_pb2.TaskRequest(status='waiting')
    channel.close()


if __name__ == "__main__":
    logging.basicConfig()
    run()
