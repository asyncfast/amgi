import re

import pytest
from asyncfast._utils import _address_pattern


@pytest.mark.parametrize(
    ["address", "pattern"],
    [
        ("test", r"^test$"),
        ("test-{name}", r"^test\-(?P<name>.*)$"),
        ("user/{user_id}/signedup", r"^user/(?P<user_id>.*)/signedup$"),
    ],
)
def test_address_pattern(address: str, pattern: str) -> None:
    assert _address_pattern(address) == re.compile(pattern)
