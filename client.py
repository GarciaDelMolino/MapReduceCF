from __future__ import print_function
from time import sleep, time
import os
import logging
import json
import string
import re

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc

log_fn = print  # logging.info


def clean_words(ln):
    ln = ln.replace('\n', '')
    for c in string.punctuation:
        ln = ln.replace(c, '')
    ln = re.split(r' +', ln)
    return [w for w in ln if len(w.strip())]


def map_file(metadata):
    """
    Reads words in file and writes into the relevant bucket.

    :param metadata: dct containing files to read, task ID and number of buckets.
    :return: None
    """
    files = metadata['files'].split(',')
    task = metadata['taskID']
    M = metadata['buckets']
    path = os.path.dirname(files[0])

    def make_output_file(word):
        return os.path.join(path, 'intermediate', f'mr-{task}-{ord(word[0]) % M}')

    log_fn(f"MAP{task}: Extract words from {files} and write into {M} buckets")
    for file in files:
        if not os.path.exists(file):
            continue
        with open(file, 'r') as f_in:
            for ln in f_in:
                words = clean_words(ln)
                for w in words:
                    with open(make_output_file(w.lower()), 'a') as f_out:
                        f_out.write(w + '\n')
    return None


def reduce(metadata):
    """
    Reads words from buckets and writes count in output file.

    :param metadata: dct containing files to read, taskID and case sensitive flag.
    :return: None
    """

    files = metadata['files'].split(',')
    path = os.path.dirname(os.path.dirname(files[0]))
    task = metadata['taskID']
    cased = metadata['case_sensitive']
    if cased:
        uncase = lambda x: x
    else:
        uncase = lambda x: x.lower()

    log_fn(f"REDUCE{task}: Count words from {files} with case sensitive {cased}")

    word_count = {}
    for file in files:
        if not os.path.exists(file):
            continue
        with open(file, 'r') as f_in:
            for ln in f_in:
                w = uncase(ln[:-1])
                word_count[w] = word_count.get(w, 0) + 1

    with open(os.path.join(path, 'out', f'out-{task}'), 'w') as f_out:
        for k, v in word_count.items():
            f_out.write(f'{k} {v}\n')
    return None


def run():
    """
    Client to process tasks as instructed by the driver.
    The tasks can be MAP, REDUCE, WAIT or SHUTDOWN
    The client will wait for the driver to be online up to a timeout.
    It will only exit in case of timeout (server is offline) or upon reception of the SHUTDOWN instruction.

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
        # client waits for a task until SHUTDOWN signal is received.
        try:
            response = stub.GetTask(task_req)
        except grpc._channel._InactiveRpcError:
            # there's probably a better way to handle this timeout using grpc kwargs
            log_fn("Waiting for server connection")
            sleep(retry_time)
            if (time() - t0) > timeout:
                logging.warning("Could not establish connection. Exiting.")
                break
            continue

        print(f"Task received: {response.task} {response.metadata}")
        # do stuff according to task
        if response.task == 'SHUTDOWN':
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
            sleep(retry_time)
            task_req = mapreduce_pb2.TaskRequest(status='waiting')
    channel.close()


if __name__ == "__main__":
    logging.basicConfig()
    run()
