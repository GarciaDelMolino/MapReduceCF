# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import mapreduce_pb2 as mapreduce__pb2


class DriverStub(object):
    """The driver service definition.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetTask = channel.unary_unary(
                '/mapreduce.Driver/GetTask',
                request_serializer=mapreduce__pb2.TaskRequest.SerializeToString,
                response_deserializer=mapreduce__pb2.TaskReply.FromString,
                )


class DriverServicer(object):
    """The driver service definition.
    """

    def GetTask(self, request, context):
        """Sends a task
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DriverServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetTask': grpc.unary_unary_rpc_method_handler(
                    servicer.GetTask,
                    request_deserializer=mapreduce__pb2.TaskRequest.FromString,
                    response_serializer=mapreduce__pb2.TaskReply.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'mapreduce.Driver', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Driver(object):
    """The driver service definition.
    """

    @staticmethod
    def GetTask(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/mapreduce.Driver/GetTask',
            mapreduce__pb2.TaskRequest.SerializeToString,
            mapreduce__pb2.TaskReply.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)