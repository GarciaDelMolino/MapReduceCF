from __future__ import print_function
from time import sleep, time
import os
import logging
import json
import string
import re
from pathlib import Path
import pickle

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc
import numpy as np

log_fn = print  # logging.info


def clean_words(ln):
    ln = ln.replace('\n', '')
    for c in string.punctuation:
        ln = ln.replace(c, '')
    ln = re.split(r' +', ln)
    return [w for w in ln if len(w.strip())]


def map_op(metadata):
    """
    Scans files in directory and writes into the relevant bucket.

    :param metadata: dct containing folders to read, task ID and number of buckets.
    :return: None
    """
    folders = metadata['directory'].split(',')
    task = metadata['taskID']
    M = metadata['buckets']
    target_extensions = [ext.strip() for ext in metadata['extension'].split(',')]
    with open(metadata['directory_ids'], 'r') as f:
        folder_tracker = json.load(f)
    folder_tracker = {folder: int(i) for i, folder in folder_tracker.items()}
    path = os.path.dirname(metadata['directory_ids'])

    def make_output_file(filename):
        return os.path.join(path, 'intermediate', f'mr-{task}-{ord(filename[-1]) % M}')

    log_fn(f"MAP{task}: Scan files in {folders} and write into {M} buckets")
    for folder in folders:
        if not os.path.exists(folder):
            continue
        folder_id = folder_tracker[folder]
        for ext in target_extensions:
            for ln in Path(folder).glob(f"*{ext}"):
                filename = ln.stem
                with open(make_output_file(filename.lower()), 'a') as f_out:
                    f_out.write(f'{folder_id}\t{filename}\n')
    return None


def reduce_op(metadata):
    """
    Reads words from buckets and writes count in output file.

    :param metadata: dct containing files to read, taskID and case sensitive flag.
    :return: None
    """

    files = metadata['directory'].split(',')
    with open(metadata['directory_ids'], 'r') as f:
        folder_tracker = json.load(f)
    path = os.path.dirname(metadata['directory_ids'])
    task = metadata['taskID']

    log_fn(f"REDUCE{task}: Count repetitions from {files}")

    repetitions = {}
    for file in files:
        if not os.path.exists(file):
            continue
        with open(file, 'r') as f_in:
            for ln in f_in:
                folder_id, filename = ln[:-1].split('\t')
                repetitions[filename] = repetitions.get(filename, []) + [int(folder_id)]

    repetitions = {k: v for k, v in repetitions.items() if len(v) > 1}
    # write repeated files sorted by number of repetitions:
    with open(os.path.join(path, 'out', f'out-{task}'), 'w') as f_out:
        for k, v in sorted(repetitions.items(), key=lambda item: len(item[1])):
            f_out.write(f'{k}: {v}\n')

    # find most overlapped folders:
    repetition_matrix = [[0 for _ in range(len(folder_tracker))]  for _ in range(len(folder_tracker))]
    for v in repetitions.values():
        for i, vi in enumerate(v):
            for vj in v[(i+1):]:
                repetition_matrix[vi][vj] += 1
                repetition_matrix[vj][vi] += 1
    with open(os.path.join(path, 'out', f'repetition_matrix-{task}.pkl'), 'wb') as f_out:
        pickle.dump(np.array(repetition_matrix), f_out)

    return None


def final_op(metadata):
    M = metadata['buckets']
    with open(metadata['directory_ids'], 'r') as f:
        folder_tracker = json.load(f)
    folder_tracker = {int(k): v for k, v in folder_tracker.items()}
    path = os.path.dirname(metadata['directory_ids'])

    repetition_matrix = np.zeros((len(folder_tracker), len(folder_tracker)))
    for task in range(M):
        with open(os.path.join(path, 'out', f'repetition_matrix-{task}.pkl'), 'rb') as f:
            repetition_matrix += pickle.load(f)
    max_rep = np.max(repetition_matrix, axis=0)
    total_rep = np.sum(repetition_matrix, axis=0)
    total_similar_folders = np.sum(repetition_matrix > 0, axis=0)
    ranked_similar_folders = np.argsort(repetition_matrix, axis=1)[:, ::-1]

    with open(os.path.join(path, 'repeated_paths_report.txt'), 'w') as f_out:
        f_out.write('directory\trepetitions\tmax repeated files\ttotal repeated files\tranked overlapped folders\n')
        for i in np.argsort(total_similar_folders)[::-1]:
            ts = total_similar_folders[i]
            if not ts:
                # contents in folder are not repeated
                break
            dir_name = folder_tracker[i]
            mr, tr, rsf = max_rep[i], total_rep[i], ranked_similar_folders[i]
            similar_folders = [f'{folder_tracker[sf]}: {repetition_matrix[sf,i]}' for sf in rsf if repetition_matrix[sf,i] > 0]
            f_out.write(f'{dir_name}\t{ts}\t{mr}\t{tr}\t{"; ".join(similar_folders)}\n')
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
            map_op(metadata)
            task_req = mapreduce_pb2.TaskRequest(status=f'done MAP{metadata.get("taskID", -1)}')
        elif response.task == 'REDUCE':
            metadata = json.loads(response.metadata)
            reduce_op(metadata)
            task_req = mapreduce_pb2.TaskRequest(status=f'done REDUCE{metadata.get("taskID", -1)}')
        elif response.task == 'WAIT':
            sleep(retry_time)
            task_req = mapreduce_pb2.TaskRequest(status='waiting')
        elif response.task == 'WRAP-UP':
            metadata = json.loads(response.metadata)
            final_op(metadata)
            log_fn("Task completed and wrapped up. Exiting.")
            break

    channel.close()


if __name__ == "__main__":
    logging.basicConfig()
    run()
