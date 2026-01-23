from typing import Any


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    if exitstatus == 5:
        session.exitstatus = 0
