from concurrent import futures
import logging

import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc


class Driver(mapreduce_pb2_grpc.DriverServicer):
    def GetTask(self, request, context):
        # do things here.
        return mapreduce_pb2.TaskReply(task='KILL', input_file='', output_file='')

def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_DriverServicer_to_server(Driver(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()