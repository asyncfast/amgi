from unittest.mock import Mock

app = Mock()

app.asyncapi.return_value = {
    "asyncapi": "3.0.0",
    "info": {"title": "AsyncFast", "version": "0.1.0"},
    "channels": {},
    "operations": {},
    "components": {"messages": {}},
}
