from abc import ABC
from typing import ClassVar

from pydantic.fields import FieldInfo


class Binding(FieldInfo, ABC):
    __protocol__: ClassVar[str]
    __field_name__: ClassVar[str]


class KafkaKey(Binding):
    __protocol__ = "kafka"
    __field_name__ = "key"
