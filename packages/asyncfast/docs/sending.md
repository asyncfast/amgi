# Sending

Sending can be achieved by yielding a message to send:

```{eval-rst}
.. async-fast-example:: examples/send_dict.py
```

The most dynamic method is to use a `dict`, with the `dict` form being:

:address (`str`): The address to send the message
:payload (`bytes | None`): The payload of the message to send
:headers (`list[tuple[bytes, bytes]]`): The headers to send with the message

While this is useful, the type cannot be inferred for documentation generation. To do this the class `Message` must be
used.

## Message

The `Message` class uses the classes annotations to discover the types of a message. The equivalent of the above example
would be:

```{eval-rst}
.. async-fast-example:: examples/send_message.py
```

This follows the same rules as setting up a receiver. For example, one payload, and using annotated types for headers.
