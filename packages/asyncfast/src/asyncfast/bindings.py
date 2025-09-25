from abc import ABC
from collections.abc import Sequence
from typing import ClassVar

from pydantic.fields import FieldInfo


class Binding(FieldInfo, ABC):
    __path__: ClassVar[Sequence[str]]


class KafkaKey(Binding):
    __path__: ClassVar[Sequence[str]] = ("kafka", "key")
