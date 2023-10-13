load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("//bazel:grpc_build_system.bzl", "grpc_proto_library")
load("//bazel:python_rules.bzl", "py_grpc_library", "py_proto_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])


# The following three rules demonstrate the usage of the cc_grpc_library rule in
# in a mode compatible with the native proto_library and cc_proto_library rules.
proto_library(
    name = "mapreduce_proto",
    srcs = ["mapreduce.proto"],
)

cc_proto_library(
    name = "mapreduce_cc_proto",
    deps = [":mapreduce_proto"],
)

cc_grpc_library(
    name = "mapreduce_cc_grpc",
    srcs = [":mapreduce_proto"],
    grpc_only = True,
    deps = [":mapreduce_cc_proto"],
)


py_proto_library(
    name = "mapreduce_py_pb2",
    deps = [":mapreduce_proto"],
)

py_grpc_library(
    name = "mapreduce_py_pb2_grpc",
    srcs = [":mapreduce_proto"],
    deps = [":mapreduce_py_pb2"],
)
