from unittest.mock import Mock
from unittest.mock import patch

from amgi_nats_py import core
from amgi_nats_py.core import _run_cli


def test_run_cli_defaults() -> None:
    with patch.object(core, "run") as mock_run:
        mock_app = Mock()
        _run_cli(mock_app, ["subject"])
        mock_run.assert_called_once_with(
            mock_app, "subject", servers=["nats://localhost:4222"]
        )
