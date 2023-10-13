from __future__ import print_function
from time import sleep, time

import logging

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc


def map_file(filenames, bucket_map):
    """
    Reads words in file and writes into the relevant bucket.

    :param filenames: files to read
    :param bucket_map: dict with the bucket id for each letter
    :return: None
    """

    return None


def reduce(filenames, output_file):
    """
    Reads words from buckets and writes count in output file.

    :param filenames: buckets to read from
    :param output_file: file to write word count to
    :return: None
    """

    return None

def run():
    # client will only close when the driver instructs so.
    print("Will try to connect ...")
    timeout = 60
    retry_time = 1
    t0 = time()
    channel = grpc.insecure_channel("localhost:50051")
    stub = mapreduce_pb2_grpc.DriverStub(channel)
    task_req = mapreduce_pb2.TaskRequest(status='new')
    while True:
        try:
            response = stub.GetTask(task_req)
        except grpc._channel._InactiveRpcError:
            # there's probably a better way to handle this timeout using GetTask kwargs
            print("Waiting for server connection")
            sleep(retry_time)
            if (time()-t0) > timeout:
                print("Could not establish connection. Exiting.")
                break
            continue

        print(f"Task received: apply {response.task} on file {response.file}")
        # do stuff according to task
        if response.task == 'KILL':
            print("Job is completed. Exiting.")
            break
        elif response.task == 'MAP':
            map_file(response.input_file, response.output_file)
            task_req = mapreduce_pb2.TaskRequest(status='done mapping')
        elif response.task == 'REDUCE':
            reduce(response.input_file, response.output_file)
            task_req = mapreduce_pb2.TaskRequest(status='done reduce')

    channel.close()


if __name__ == "__main__":
    logging.basicConfig()
    run()
