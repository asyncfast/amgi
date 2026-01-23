import json
import sys
from pathlib import Path

from asyncfast_cli.cli import app
from typer.testing import CliRunner

runner = CliRunner()


def test_asyncapi() -> None:
    sys.path.insert(0, str(Path(__file__).parent))
    result = runner.invoke(app, ["asyncapi", "main:app"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "asyncapi": "3.0.0",
        "info": {"title": "AsyncFast", "version": "0.1.0"},
        "channels": {},
        "operations": {},
        "components": {"messages": {}},
    }
