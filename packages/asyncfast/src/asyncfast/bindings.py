from abc import ABC
from typing import Any
from typing import ClassVar

from pydantic import TypeAdapter
from pydantic.fields import FieldInfo


class Binding(FieldInfo, ABC):
    __protocol__: ClassVar[str]
    __field_name__: ClassVar[str]

    def dump_value(self, value: Any, type_adapter: TypeAdapter[Any]) -> Any:
        return type_adapter.dump_python(value, mode="json")


class KafkaKey(Binding):
    __protocol__ = "kafka"
    __field_name__ = "key"

    def dump_value(self, value: Any, type_adapter: TypeAdapter[Any]) -> Any:
        python = type_adapter.dump_python(value, mode="json")
        if isinstance(python, str):
            return python.encode()
        return type_adapter.dump_json(value)


class SqsDelaySeconds(Binding):
    __protocol__ = "sqs"
    __field_name__ = "delay_seconds"


class SqsMessageDeduplicationId(Binding):
    __protocol__ = "sqs"
    __field_name__ = "message_deduplication_id"


class SqsMessageGroupId(Binding):
    __protocol__ = "sqs"
    __field_name__ = "message_group_id"
