from amgi_cloudevents._message_send import ContentMode
from amgi_cloudevents._message_send import MessageSend
from amgi_cloudevents._server import Server
from amgi_types import AMGIApplication


def run(
    app: AMGIApplication,
    host: str = "0.0.0.0",
    port: int = 8000,
    path: str = "/event",
    message_send_endpoint: str | None = None,
    message_send_source: str = "/amgi-cloudevents",
    message_send_content_mode: ContentMode = "structured",
    message_send_log_missing: bool = False,
) -> None:
    import uvicorn

    message_send = (
        MessageSend(
            message_send_endpoint,
            source=message_send_source,
            content_mode=message_send_content_mode,
        )
        if message_send_endpoint is not None
        else None
    )
    uvicorn.run(
        Server(
            app,
            path=path,
            message_send=message_send,
            message_send_log_missing=message_send_log_missing,
        ),
        host=host,
        port=port,
    )
