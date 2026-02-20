import re
from collections import Counter
from re import Pattern

_FIELD_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
_PARAMETER_PATTERN = re.compile(r"{(.*)}")


def get_address_parameters(address: str | None) -> set[str]:
    if address is None:
        return set()
    parameters = _PARAMETER_PATTERN.findall(address)
    for parameter in parameters:
        assert _FIELD_PATTERN.match(parameter), f"Parameter '{parameter}' is not valid"

    duplicates = {item for item, count in Counter(parameters).items() if count > 1}
    assert len(duplicates) == 0, f"Address contains duplicate parameters: {duplicates}"
    return set(parameters)


def get_address_pattern(address: str) -> Pattern[str]:
    index = 0
    address_regex = "^"
    for match in _PARAMETER_PATTERN.finditer(address):
        (name,) = match.groups()
        address_regex += re.escape(address[index : match.start()])
        address_regex += f"(?P<{name}>.*)"

        index = match.end()

    address_regex += re.escape(address[index:]) + "$"
    return re.compile(address_regex)
