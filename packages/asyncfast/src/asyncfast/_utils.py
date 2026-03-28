import re
from collections import Counter
from re import Pattern
from typing import Generic
from typing import TypeVar

T = TypeVar("T")

FIELD_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
PARAMETER_PATTERN = re.compile(r"{(.*)}")


def get_address_parameters(address: str | None) -> set[str]:
    if address is None:
        return set()
    parameters = PARAMETER_PATTERN.findall(address)
    for parameter in parameters:
        assert FIELD_PATTERN.match(parameter), f"Parameter '{parameter}' is not valid"

    duplicates = {item for item, count in Counter(parameters).items() if count > 1}
    assert len(duplicates) == 0, f"Address contains duplicate parameters: {duplicates}"
    return set(parameters)


def get_address_pattern(address: str) -> Pattern[str]:
    index = 0
    address_regex = "^"
    for match in PARAMETER_PATTERN.finditer(address):
        (name,) = match.groups()
        address_regex += re.escape(address[index : match.start()])
        address_regex += f"(?P<{name}>.*)"

        index = match.end()

    address_regex += re.escape(address[index:]) + "$"
    return re.compile(address_regex)


class ChannelNotFoundError(LookupError):
    def __init__(self, address: str) -> None:
        super().__init__(f"Couldn't resolve address: {address}")
        self.address = address


class Router(Generic[T]):
    def __init__(self) -> None:
        self.routes: list[tuple[Pattern[str], T]] = []

    def add_route(self, address: str, route: T) -> None:
        self.routes.append((get_address_pattern(address), route))

    def get(self, address: str) -> tuple[dict[str, str], T]:
        for address_pattern, route in self.routes:
            parameters = address_pattern.match(address)
            if parameters is not None:
                return parameters.groupdict(), route
        raise ChannelNotFoundError(address)
