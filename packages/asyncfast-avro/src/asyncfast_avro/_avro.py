from __future__ import annotations

import re
from collections.abc import Callable
from collections.abc import Hashable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from io import BytesIO
from typing import Any
from typing import cast
from typing import TypeVar
from typing import Union

from asyncfast import InvalidChannelDefinitionError
from asyncfast import Payload
from fastavro import parse_schema
from fastavro import schemaless_reader
from fastavro import schemaless_writer
from pydantic import TypeAdapter
from pydantic.json_schema import JsonSchemaValue

AVRO_SCHEMA_FORMAT = "application/vnd.apache.avro;version=1.9.0"

AvroSchema = Union[str, list[Any], dict[str, Any]]

T = TypeVar("T")

_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TIMEDELTA_ADAPTER = TypeAdapter(timedelta)

_TYPES: dict[str, str] = {
    "boolean": "boolean",
    "integer": "long",
    "number": "double",
    "null": "null",
    "string": "string",
}
_FORMATS: dict[str, AvroSchema] = {
    "binary": "bytes",
    "date": {"type": "int", "logicalType": "date"},
    "date-time": {"type": "long", "logicalType": "timestamp-micros"},
    "time": {"type": "long", "logicalType": "time-micros"},
    "uuid": {"type": "string", "logicalType": "uuid"},
}
_NAMED_TYPES = ("enum", "fixed", "record")


class _GenerateAvroSchema:
    """
    Generates an Avro schema from the JSON schema pydantic produces for a type.

    Deriving from the JSON schema rather than the core schema keeps the mapping stable across pydantic versions, and
    keeps Avro payloads described by the same types as JSON ones.
    """

    def __init__(self) -> None:
        self.definitions: dict[str, JsonSchemaValue] = {}
        # The definition each Avro named type was first emitted with, so later uses of the same name can
        # reference it. Definitions are kept with already defined types replaced by references to their
        # name, so a definition built at any use compares equal to the one first emitted
        self.named: dict[str, dict[str, Any]] = {}
        self.building: set[str] = set()

    def generate(self, json_schema: JsonSchemaValue) -> AvroSchema:
        self.definitions = json_schema.get("$defs", {})
        return self._generate(json_schema)

    def _generate(self, schema: JsonSchemaValue) -> AvroSchema:
        if "$ref" in schema:
            return self._reference(schema["$ref"])
        if "allOf" in schema and len(schema["allOf"]) == 1:
            return self._generate(schema["allOf"][0])
        for keyword in ("anyOf", "oneOf"):
            if keyword in schema:
                return self._union(self._generate(choice) for choice in schema[keyword])
        if "const" in schema:
            return self._symbols(schema, [schema["const"]])
        if "enum" in schema:
            return self._symbols(schema, schema["enum"])

        schema_type = schema.get("type")
        if schema_type == "array":
            return self._array(schema)
        if schema_type == "object":
            return self._object(schema)
        if schema_type == "string" and schema.get("format") in _FORMATS:
            format_ = _FORMATS[schema["format"]]
            return dict(format_) if isinstance(format_, dict) else format_
        if schema_type in _TYPES:
            return _TYPES[schema_type]
        raise InvalidChannelDefinitionError(
            f"Cannot generate an Avro schema for {schema!r}"
        )

    def _reference(self, reference: str) -> AvroSchema:
        name = reference.rsplit("/", 1)[-1]
        definition = self.definitions[name]
        if "enum" in definition or "const" in definition:
            return self._generate(definition)
        return self._record(definition.get("title", name), definition)

    def _named(self, name: str, definition: Callable[[], dict[str, Any]]) -> AvroSchema:
        """
        Avro named types are defined once, then referenced by name.

        A second definition of the same name must match the first, otherwise the schema would silently
        encode one of the types with the other's symbols or fields.
        """
        if name in self.building:
            return name  # a recursive reference to the type currently being defined
        existing = self.named.get(name)
        if existing is None:
            self.building.add(name)
            built = definition()
            self.building.discard(name)
            self.named[name] = self._canonical(built)
            return built
        self.building.add(name)
        candidate = definition()
        self.building.discard(name)
        if candidate != existing:
            raise InvalidChannelDefinitionError(
                f"Conflicting Avro definitions for {name!r}: rename one of the pydantic models or fields"
            )
        return name

    def _canonical(self, schema: Any) -> Any:
        """Replace inline definitions of named types with a reference to their name."""
        if isinstance(schema, str):
            return schema
        if isinstance(schema, list):
            return [self._canonical(branch) for branch in schema]
        if isinstance(schema, dict):
            if schema.get("type") in _NAMED_TYPES and schema.get("name") in self.named:
                return schema["name"]
            return {key: self._canonical(value) for key, value in schema.items()}
        return schema

    def _symbols(self, schema: JsonSchemaValue, values: Iterable[Any]) -> AvroSchema:
        values = list(values)
        name = schema.get("title")
        unsupported = [value for value in values if type(value) not in _JSON_TYPES]
        if unsupported or not values:
            raise InvalidChannelDefinitionError(
                f"Cannot generate an Avro schema for enum {name or schema!r} with no encodable values"
            )
        symbols = [value for value in values if isinstance(value, str)]
        if (
            name is None
            or len(symbols) != len(values)
            or not all(_NAME_PATTERN.match(symbol) for symbol in symbols)
        ):
            return self._union([_TYPES[_JSON_TYPES[type(value)]] for value in values])
        return self._named(
            name, lambda: {"type": "enum", "name": name, "symbols": symbols}
        )

    def _array(self, schema: JsonSchemaValue) -> AvroSchema:
        if "prefixItems" in schema:
            return {
                "type": "array",
                "items": self._union(
                    self._generate(item) for item in schema["prefixItems"]
                ),
            }
        items = schema.get("items")
        if items is None:
            raise InvalidChannelDefinitionError(
                "Cannot generate an Avro schema for an untyped collection"
            )
        return {"type": "array", "items": self._generate(items)}

    def _object(self, schema: JsonSchemaValue) -> AvroSchema:
        additional_properties = schema.get("additionalProperties")
        if "properties" not in schema:
            if not isinstance(additional_properties, dict):
                raise InvalidChannelDefinitionError(
                    "Cannot generate an Avro schema for an untyped mapping"
                )
            return {"type": "map", "values": self._generate(additional_properties)}
        name = schema.get("title")
        if name is None:
            raise InvalidChannelDefinitionError(
                "Cannot generate an Avro schema for an unnamed record"
            )
        return self._record(name, schema)

    def _record(self, name: str, schema: JsonSchemaValue) -> AvroSchema:
        return self._named(
            name,
            lambda: {
                "type": "record",
                "name": name,
                "fields": list(self._fields(schema)),
            },
        )

    def _fields(self, schema: JsonSchemaValue) -> Iterator[dict[str, Any]]:
        for name, property_schema in schema.get("properties", {}).items():
            field: dict[str, Any] = {
                "name": name,
                "type": self._generate(property_schema),
            }
            if "default" in property_schema:
                self._default(field, property_schema["default"])
            yield field

    def _default(self, field: dict[str, Any], default: Any) -> None:
        """
        An Avro default must validate against the first branch of a union, so the branch matching the
        default is moved to the front. Defaults of nested records are not emitted: they would require
        expanding the default of every field of the record.
        """
        json_type = _JSON_TYPES.get(type(default))
        field_type = field["type"]
        if isinstance(field_type, list) and json_type is not None:
            for branch in field_type:
                if isinstance(branch, str) and _AVRO_TYPES.get(branch) == json_type:
                    field["type"] = [
                        branch,
                        *[other for other in field_type if other is not branch],
                    ]
                    field["default"] = default
                    return
        elif isinstance(field_type, str):
            if json_type is not None and _AVRO_TYPES.get(field_type) == json_type:
                field["default"] = default
            elif _is_enum(self.named.get(field_type)):
                # an enum default is its symbol, which is the str of its value
                field["default"] = default
        elif _is_enum(field_type):
            field["default"] = default

    def _union(self, choices: Iterable[AvroSchema]) -> AvroSchema:
        branches: list[AvroSchema] = []
        for choice in choices:
            # Avro unions cannot immediately contain other unions
            for branch in choice if isinstance(choice, list) else [choice]:
                if branch not in branches:
                    branches.append(branch)
        if len(branches) == 1:
            return branches[0]
        # A union default must match its first branch, which is null by convention
        branches.sort(key=lambda branch: branch != "null")
        return branches


_JSON_TYPES: dict[type[Any], str] = {
    type(None): "null",
    bool: "boolean",
    int: "integer",
    float: "number",
    str: "string",
}
_AVRO_TYPES = {avro: json for json, avro in _TYPES.items()}


def _is_enum(definition: Any) -> bool:
    return isinstance(definition, dict) and definition.get("type") == "enum"


@lru_cache(maxsize=None)
def _avro_schema(type_adapter: Hashable) -> AvroSchema:
    # An Avro schema describes the encoded bytes, so one schema serves both sending and
    # receiving, unlike the JSON schemas generated per mode
    json_schema = cast(TypeAdapter[Any], type_adapter).json_schema(mode="serialization")
    return _GenerateAvroSchema().generate(json_schema)


@lru_cache(maxsize=None)
def _parsed_schema(type_adapter: Hashable) -> Any:
    return parse_schema(_avro_schema(type_adapter))


def _avro_dumps(type_adapter: TypeAdapter[Any], value: Any) -> bytes:
    buffer = BytesIO()
    try:
        schemaless_writer(buffer, _parsed_schema(type_adapter), _encodable(value))
    except (EOFError, IndexError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Avro value: {exc}") from exc
    return buffer.getvalue()


def _avro_loads(type_adapter: TypeAdapter[Any], data: bytes) -> Any:
    reader = BytesIO(data)
    try:
        value = schemaless_reader(reader, _parsed_schema(type_adapter))
    except (EOFError, IndexError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Avro payload: {exc}") from exc
    if reader.tell() != len(data):
        raise ValueError(
            f"Invalid Avro payload: {len(data) - reader.tell()} unexpected trailing bytes"
        )
    return value


def _encodable(value: Any) -> Any:
    """Coerce the values pydantic dumps that fastavro will not encode as-is."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        # fastavro interprets naive datetimes in the local timezone, so they are normalized to
        # UTC, giving the same encoding wherever the payload is produced
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, timedelta):
        return _TIMEDELTA_ADAPTER.dump_python(value, mode="json")
    if isinstance(value, Mapping):
        # Avro map keys are always strings
        return {str(key): _encodable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_encodable(item) for item in value]
    return value


class AvroPayload(Payload):  # type: ignore[misc]
    __schema_format__ = AVRO_SCHEMA_FORMAT

    def dump_value(self, value: Any, type_adapter: TypeAdapter[Any]) -> bytes:
        return _avro_dumps(type_adapter, type_adapter.dump_python(value))

    def load_value(self, type_adapter: TypeAdapter[T], payload: bytes) -> T:
        return type_adapter.validate_python(_avro_loads(type_adapter, payload))

    def asyncapi_schema(self, type_adapter: TypeAdapter[Any]) -> AvroSchema:
        return _avro_schema(type_adapter)
