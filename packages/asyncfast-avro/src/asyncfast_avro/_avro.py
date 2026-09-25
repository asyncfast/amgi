from __future__ import annotations

import re
from collections.abc import Hashable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from datetime import timedelta
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


class GenerateAvroSchema:
    """
    Generates an Avro schema from the JSON schema pydantic produces for a type.

    Deriving from the JSON schema rather than the core schema keeps the mapping stable across pydantic versions, and
    keeps Avro payloads described by the same types as JSON ones.
    """

    def __init__(self) -> None:
        self.definitions: dict[str, JsonSchemaValue] = {}
        self.names: set[str] = set()

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

    def _named(self, name: str) -> str | None:
        """Avro named types are defined once, then referenced by name."""
        if name in self.names:
            return name
        self.names.add(name)
        return None

    def _symbols(self, schema: JsonSchemaValue, values: Iterable[Any]) -> AvroSchema:
        symbols = [str(value) for value in values]
        name = schema.get("title")
        if name is None or not all(_NAME_PATTERN.match(symbol) for symbol in symbols):
            return self._union(
                _TYPES[_JSON_TYPES[type(value)]]
                for value in values
                if type(value) in _JSON_TYPES
            )
        return self._named(name) or {"type": "enum", "name": name, "symbols": symbols}

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
        return self._named(name) or {
            "type": "record",
            "name": name,
            "fields": list(self._fields(schema)),
        }

    def _fields(self, schema: JsonSchemaValue) -> Iterator[dict[str, Any]]:
        for name, property_schema in schema.get("properties", {}).items():
            field: dict[str, Any] = {
                "name": name,
                "type": self._generate(property_schema),
            }
            if "default" in property_schema:
                default = property_schema["default"]
                branch = field["type"]
                avro_type = branch[0] if isinstance(branch, list) else branch
                # Named types carry no JSON type to match a default against
                if isinstance(avro_type, str) and _JSON_TYPES.get(
                    type(default)
                ) == _AVRO_TYPES.get(avro_type):
                    field["default"] = default
            yield field

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


def avro_schema(type_adapter: TypeAdapter[Any]) -> AvroSchema:
    return _avro_schema(type_adapter)


def avro_dumps(type_adapter: TypeAdapter[Any], value: Any) -> bytes:
    buffer = BytesIO()
    schemaless_writer(buffer, _parsed_schema(type_adapter), _encodable(value))
    return buffer.getvalue()


def avro_loads(type_adapter: TypeAdapter[Any], data: bytes) -> Any:
    return schemaless_reader(BytesIO(data), _parsed_schema(type_adapter))


@lru_cache(maxsize=None)
def _avro_schema(type_adapter: Hashable) -> AvroSchema:
    # An Avro schema describes the encoded bytes, so one schema serves both sending and
    # receiving, unlike the JSON schemas generated per mode
    json_schema = cast(TypeAdapter[Any], type_adapter).json_schema(mode="serialization")
    return GenerateAvroSchema().generate(json_schema)


@lru_cache(maxsize=None)
def _parsed_schema(type_adapter: Hashable) -> Any:
    return parse_schema(_avro_schema(type_adapter))


def _encodable(value: Any) -> Any:
    """Coerce the values pydantic dumps that fastavro will not encode as-is."""
    if isinstance(value, Enum):
        return value.value
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
        return avro_dumps(type_adapter, type_adapter.dump_python(value))

    def load_value(self, type_adapter: TypeAdapter[T], payload: bytes) -> T:
        return type_adapter.validate_python(avro_loads(type_adapter, payload))

    def asyncapi_schema(self, type_adapter: TypeAdapter[Any]) -> AvroSchema:
        return avro_schema(type_adapter)
