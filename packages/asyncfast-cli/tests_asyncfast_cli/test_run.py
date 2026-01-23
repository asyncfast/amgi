import sys
from importlib import metadata
from pathlib import Path
from typing import Generator
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from amgi_types import AMGIApplication
from typer.testing import CliRunner


@pytest.fixture
def mock_entry_points_select() -> Generator[Mock, None, None]:
    with patch.object(metadata, "entry_points") as mock_entry_points:
        yield mock_entry_points.return_value.select


runner = CliRunner()


def test_run_app(mock_entry_points_select: Mock) -> None:
    sys.path.insert(0, str(Path(__file__).parent))

    mock_run = Mock()

    def _run(app: AMGIApplication) -> None:
        mock_run(app)

    mock_entry_point = Mock()
    mock_entry_point.name = "test"
    mock_entry_point.load.return_value = _run

    mock_entry_points_select.return_value = [mock_entry_point]

    sys.modules.pop("asyncfast_cli.cli", None)

    from asyncfast_cli.cli import app

    result = runner.invoke(app, ["run", "test", "main:app"])
    assert result.exit_code == 0

    assert mock_run.mock_calls[0].args[0].asyncapi() == {
        "asyncapi": "3.0.0",
        "channels": {},
        "components": {"messages": {}},
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "operations": {},
    }


def test_run_app_load_failure(mock_entry_points_select: Mock) -> None:
    mock_entry_point = Mock()
    mock_entry_point.load.side_effect = RuntimeError

    mock_entry_points_select.return_value = [mock_entry_point]

    sys.modules.pop("asyncfast_cli.cli", None)

    from asyncfast_cli.cli import app

    result = runner.invoke(app, ["run", "test", "main:app"])
    assert result.exit_code != 0
