from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import get_args
from typing import get_origin

from asyncfast._channel import Header
from asyncfast._channel import Parameter
from asyncfast._channel import Payload
from asyncfast._utils import get_address_parameters
from asyncfast.bindings import Binding
from pydantic import TypeAdapter


class _Field:
    def __init__(self, type_: type):
        self.type = type_
        self.type_adapter = TypeAdapter[Any](type_)

    def __hash__(self) -> int:
        return hash(self.type)


class Message(Mapping[str, Any]):

    __address__: ClassVar[str | None] = None
    __headers__: ClassVar[dict[str, tuple[str, _Field]]]
    __parameters__: ClassVar[dict[str, TypeAdapter[Any]]]
    __payload__: ClassVar[tuple[str, _Field] | None]
    __bindings__: ClassVar[dict[str, tuple[str, str, _Field]]]

    def __init_subclass__(cls, address: str | None = None, **kwargs: Any) -> None:
        cls.__address__ = address
        annotations = list(_generate_message_annotations(address, cls.__annotations__))

        headers = {
            name: (
                (
                    get_args(annotated)[1].alias
                    if get_args(annotated)[1].alias
                    else name.replace("_", "-")
                ),
                _Field(annotated),
            )
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Header)
        }

        parameters = {
            name: TypeAdapter(annotated)
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Parameter)
        }

        bindings = {
            name: (
                get_args(annotated)[1].__protocol__,
                get_args(annotated)[1].__field_name__,
                _Field(annotated),
            )
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Binding)
        }

        payloads = [
            (name, _Field(annotated))
            for name, annotated in annotations
            if isinstance(get_args(annotated)[1], Payload)
        ]

        assert len(payloads) <= 1, "Channel must have no more than 1 payload"

        payload = payloads[0] if len(payloads) == 1 else None

        cls.__headers__ = headers
        cls.__parameters__ = parameters
        cls.__payload__ = payload
        cls.__bindings__ = bindings

    def __getitem__(self, key: str, /) -> Any:
        if key == "address":
            return self._get_address()
        elif key == "headers":
            return self._get_headers()
        elif key == "payload" and self.__payload__:
            name, field = self.__payload__
            return field.type_adapter.dump_json(getattr(self, name))
        elif key == "bindings" and self.__bindings__:
            return self._get_bindings()
        raise KeyError(key)

    def __len__(self) -> int:
        payload = 1 if self.__payload__ else 0
        bindings = 1 if self.__bindings__ else 0
        return 2 + payload + bindings

    def __iter__(self) -> Iterator[str]:
        yield from ("address", "headers")
        if self.__payload__:
            yield "payload"
        if self.__bindings__:
            yield "bindings"

    def _get_address(self) -> str | None:
        if self.__address__ is None:
            return None
        parameters = {
            name: type_adapter.dump_python(getattr(self, name))
            for name, type_adapter in self.__parameters__.items()
        }

        return self.__address__.format(**parameters)

    def _generate_headers(self) -> Iterable[tuple[str, bytes]]:
        for name, (alias, field) in self.__headers__.items():
            yield alias, self._get_value(name, field.type_adapter)

    def _get_headers(self) -> Iterable[tuple[bytes, bytes]]:
        return [(name.encode(), value) for name, value in self._generate_headers()]

    def _get_value(self, name: str, type_adapter: TypeAdapter[Any]) -> bytes:
        value = getattr(self, name)
        python = type_adapter.dump_python(value, mode="json")
        if isinstance(python, str):
            return python.encode()
        return type_adapter.dump_json(value)

    def _get_bindings(self) -> dict[str, dict[str, Any]]:
        bindings: dict[str, dict[str, Any]] = {}
        for name, (protocol, field_name, field) in self.__bindings__.items():
            bindings.setdefault(protocol, {})[field_name] = self._get_value(
                name, field.type_adapter
            )
        return bindings


def _generate_message_annotations(
    address: str | None,
    fields: dict[str, Any],
) -> Generator[tuple[str, type[Annotated[Any, Any]]], None, None]:
    address_parameters = get_address_parameters(address)
    for name, field in fields.items():
        if get_origin(field) is Annotated:
            yield name, field
        elif name in address_parameters:
            yield name, Annotated[field, Parameter()]  # type: ignore[misc]
        else:
            yield name, Annotated[field, Payload()]  # type: ignore[misc]
