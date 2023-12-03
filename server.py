from concurrent import futures
import logging
import argparse
import os
import fnmatch
import json
import queue
import threading

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc


status_tracker = {}
q_map_tasks = queue.Queue()
q_reduce_tasks = queue.Queue()
pending_maps = queue.Queue()
pending_reduce = queue.Queue()
waiting = queue.Queue()
final_task = None
map_flag = True
reduce_flag = False


log_fn = print  # logging.info

class Driver(mapreduce_pb2_grpc.DriverServicer):
    def GetTask(self, request, context):
        """
        Uses global queues to handle what task to send to the workers.
        :param request: message from client
        :param context:
        :return:
        """
        global status_tracker
        global q_map_tasks
        global q_reduce_tasks
        global pending_maps
        global pending_reduce
        global waiting
        global map_flag
        global reduce_flag
        global final_task

        # handle worker's notification (new/done MAP*/done REDUCE*)
        msg = request.status
        if msg.startswith('done'):
            log_fn("Worker finished. Message: " + msg)
            task = msg.split(' ')[-1]
            # update status tracker
            status_tracker[task] = 'done'
            # update pending tracker
            if task.startswith('MAP'):
                _ = pending_maps.get()
            elif task.startswith('REDUCE'):
                _ = pending_reduce.get()
        elif msg.startswith('waiting'):
            _ = waiting.get()

        # decide task to perform next (MAP/REDUCE/WAIT/SHUTDOWN)
        if map_flag:
            # handle MAPs first.
            try:
                task = q_map_tasks.get_nowait()
                status_tracker[f'MAP{task[0]}'] = 'working'
                reply = mapreduce_pb2.TaskReply(task='MAP', metadata=task[1])
            except queue.Empty:
                log_fn(f"Task tracker: {status_tracker}")
                map_flag = False
                # for code simplicity, make the worker wait. This is not time efficient.
                reply = mapreduce_pb2.TaskReply(task='WAIT', metadata='')
                waiting.put(1)
        elif reduce_flag:
            # handle REDUCEs if all MAPs have completed.
            try:
                task = q_reduce_tasks.get_nowait()
                status_tracker[f'REDUCE{task[0]}'] = 'working'
                reply = mapreduce_pb2.TaskReply(task='REDUCE', metadata=task[1])
            except queue.Empty:
                log_fn(f"Task tracker: {status_tracker}")
                reduce_flag = False
                # for code simplicity, make the worker wait. This is not time efficient.
                reply = mapreduce_pb2.TaskReply(task='WAIT', metadata='')
                waiting.put(1)
        else:
            # check if all workers have completed:
            if pending_reduce.empty():
                # all job is done. Send SHUTDOWN signal.
                if waiting.empty():
                    # There are no more workers waiting for instructions.
                    # Let's send "WRAP-UP" signal to the last worker.
                    reply = mapreduce_pb2.TaskReply(task='WRAP-UP', metadata=final_task)
                    # We can close shop in about 5 seconds.
                    log_fn("Job is completed and no more workers are waiting. Shutting down the server gracefully...")
                    threading.Timer(1, self.delayed_shutdown).start()
                else:
                    reply = mapreduce_pb2.TaskReply(task='SHUTDOWN', metadata='')
            elif pending_maps.empty():
                # all maps are done. Set reduce flag signal if there are reduce tasks to start.
                reduce_flag = not q_reduce_tasks.empty()
                # for code simplicity, make the worker wait. This is not time efficient.
                reply = mapreduce_pb2.TaskReply(task='WAIT', metadata='')
                waiting.put(1)
            else:
                # tasks are being processed but not completed yet. Make worker wait.
                reply = mapreduce_pb2.TaskReply(task='WAIT', metadata='')
                waiting.put(1)
        return reply

    def delayed_shutdown(self):
        global waiting
        # make sure no worker tried to connect after the shutdown instruction
        if waiting.empty():
            print("Job completed. Shutting down.")
            server.stop(0)

    def wrap_up_job(self):

        return


def find_directories_with_extension(root_dir, target_extensions):
    matching_directories = []

    # Walk through the directory tree using os.scandir
    for entry in os.scandir(root_dir):
        if entry.is_dir(follow_symlinks=False):
            # Check if any file in the current directory has the target extension
            if any(fnmatch.fnmatch(file.name, f"*{ext}") for ext in target_extensions for file in os.scandir(entry.path) if
                   file.is_file()):
                matching_directories.append(entry.path)

            # Recursive call to process subdirectories
            matching_directories.extend(find_directories_with_extension(entry.path, target_extensions))

    return matching_directories


def split_map_tasks(input_tasks, N):
    """
    Takes all input files and groups them in n_map groups

    :param input_tasks: list of input files
    :param N: number of map tasks
    :return: map tasks
    """
    output_tasks = [[] for _ in range(N)]
    for i, f in enumerate(input_tasks):
        output_tasks[(i % N)].append(f)
    output_tasks = [(task_id, ','.join(t)) for task_id, t in enumerate(output_tasks)]
    return output_tasks


def process_reduce_files(path, N, M):
    """
    Define intermediate files for the reduce operation

    :param path: files input path
    :param N: number of map tasks
    :param M: number of reduce tasks
    :return: a list containing the intermediate files to read for each reduce task.
    """
    # clean files in intermediate and out folder if they exist
    for d in ['intermediate', 'out']:
        p = os.path.join(path, d)
        if os.path.exists(p):
            for f in os.listdir(p):
                os.remove(os.path.join(p, f))
        else:
            os.mkdir(p)

    return [(m, ','.join([os.path.join(path, 'intermediate', f'mr-{n}-{m}')
                          for n in range(N)]))
            for m in range(M)]


def serve(args):
    """
    The driver defines the tasks before launching the server to receive requests.
    The files to read are split into N map tasks, and the task metadata inserted into a queue.
    The reduce tasks metadata is added to a separate queue.
    Two more queues track the tasks completion status, to give the workers instruction to stop.

    :param args: input folder, number of map and reduce tasks, case_sensitive config.
    :return:  None
    """
    global status_tracker
    global q_map_tasks
    global q_reduce_tasks
    global final_task
    global pending_maps
    global pending_reduce

    # find paths with relevant extensions
    target_extension = [ext.strip() for ext in args.extension.split(',')]
    directories = find_directories_with_extension(args.input_folder, target_extension)
    # save search meta in disk
    directory_ids = {k: v for k, v in enumerate(directories)}
    directory_ids_path = os.path.join(args.input_folder, 'directory_ids_mapper.json')
    with open(directory_ids_path, 'w') as f:
        json.dump(directory_ids, f)

    # split map tasks
    n_map = args.n_map or len(directories)
    map_tasks = split_map_tasks(directories, n_map)
    status_tracker.update({f'MAP{i}': 'pending' for i in range(n_map)})

    # reduce tasks
    reduce_tasks = process_reduce_files(args.input_folder, n_map, args.n_reduce)
    status_tracker.update({f'REDUCE{i}': 'pending' for i in range(args.n_reduce)})

    # put into queues
    for t in map_tasks:
        meta = json.dumps({'directory': t[1], 'taskID': t[0],
                           'buckets': args.n_reduce, 'extension': args.extension,
                           'directory_ids': directory_ids_path})
        q_map_tasks.put((t[0], meta))
        pending_maps.put(1)

    for t in reduce_tasks:
        meta = json.dumps({'directory': t[1], 'taskID': t[0], 'directory_ids': directory_ids_path})
        q_reduce_tasks.put((t[0], meta))
        pending_reduce.put(1)

    final_task = json.dumps({'buckets': args.n_reduce, 'directory_ids': directory_ids_path})

    # launch server
    global server
    port = "50051"                                    
    max_workers = max(args.n_map, args.n_reduce)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    mapreduce_pb2_grpc.add_DriverServicer_to_server(Driver(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parser for Map-Reduce Settings')
    parser.add_argument("--input_folder", type=str, help='Directory containing subfolders to inspect',
                        default="C:\\Users\\Ana\\Pictures")
    parser.add_argument("--n_map", type=int, help="Number of MAP tasks. Set to 0 to launch as many map tasks as folders to inspect.",
                        default=os.environ.get("N_MAP", 6))
    parser.add_argument("--n_reduce", type=int, help="Number of REDUCE operations",
                        default=os.environ.get("N_REDUCE", 4))
    parser.add_argument("--extension", type=str, help='Relevant file extensions to inspect',
                        default="jpg,png,jpeg,raw")
    args = parser.parse_args()

    logging.basicConfig()
    serve(args)