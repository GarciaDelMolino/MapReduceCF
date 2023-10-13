from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TaskRequest(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class TaskReply(_message.Message):
    __slots__ = ["task", "input_file", "output_file"]
    TASK_FIELD_NUMBER: _ClassVar[int]
    INPUT_FILE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FILE_FIELD_NUMBER: _ClassVar[int]
    task: str
    input_file: str
    output_file: str
    def __init__(self, task: _Optional[str] = ..., input_file: _Optional[str] = ..., output_file: _Optional[str] = ...) -> None: ...
