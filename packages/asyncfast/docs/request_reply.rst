###############
 Request/Reply
###############

Request/reply is where a requester sends a message and waits for a response, with the response sent back to the
requester. In AsyncFast the reply of the interaction is the message the channel handler sends, so the pattern is
declared by passing :py:class:`Reply` to the channel.

*************
 Basic Usage
*************

Marking a channel with :py:class:`Reply` adds the AsyncAPI Operation Reply object to the receive operation, where the
reply channel is the channel of the message the handler sends:

.. async-fast-example:: examples/request_reply.py

The handler must send exactly one message type, otherwise an
:py:class:`InvalidChannelDefinitionError` is raised when the channel is registered, that is, when the ``@app.channel(...)`` decorator is applied.

:py:class:`Reply` works with either sending style, so the :py:class:`MessageSender` can be used instead of yielding:

.. code-block:: python

   @app.channel("ping", reply=Reply())
   async def ping(message_sender: MessageSender[Pong]) -> None:
       await message_sender.send(Pong(payload="pong"))

***********************
 Dynamic Reply Address
***********************

When the address of the reply isn't known at design time, it is determined at runtime from the request itself. This
is typically done by the requester including a ``replyTo`` header. Marking that header with :py:class:`ReplyTo`
tells AsyncFast it carries the reply address, so the AsyncAPI reply address is derived from it and the header only
needs to be written once:

.. async-fast-example:: examples/request_reply_dynamic_address.py

:py:class:`ReplyTo` behaves like :py:class:`Header`, so the value is injected into the handler, and it can equally be
used in a dependency of the channel. A channel can have at most one :py:class:`ReplyTo` header.

The derived location is an AsyncAPI runtime expression, ``$message.header#/replyTo`` in the example above. To describe
the address, or to point at a location that isn't a header marked with :py:class:`ReplyTo`, pass a
:py:class:`ReplyAddress` to :py:class:`Reply`. The location must be either ``$message.header`` or
``$message.payload`` followed by a JSON Pointer. As a shortcut, a location can also be passed as a string:

.. code-block:: python

   @app.channel("ping", reply=Reply(address="$message.payload#/replyTo"))
   async def ping(payload: PingPayload) -> AsyncGenerator[Pong, None]:
       yield Pong(payload="pong")

The reply address is documentation only, so the handler is still responsible for sending to the address the requester
supplied. Binding the :py:class:`ReplyTo` header to a parameter of the reply channel address, as in the example above,
is the most direct way of doing this.

.. note::

   AsyncAPI 3.0 requires that, when a reply address is specified, the address of the channel referenced by the reply
   is ``null``. When :py:class:`Reply` is given an address, the reply channel is therefore rendered with a ``null``
   address, with the reply address runtime expression carrying the routing information. The reply message should not
   be reused as the send message of a standalone channel, since its channel address is rendered as ``null``.

********************
 Same Reply Address
********************

The request and the reply can also share an address, which is a reply channel using the same address as the request
channel:

.. code-block:: python

   @dataclass
   class Pong(Message, address="ping"):
       payload: str


   @app.channel("ping", reply=Reply())
   async def ping(payload: str) -> AsyncGenerator[Pong, None]:
       yield Pong(payload="pong")

.. note::

   AsyncFast generates a channel per message sent, so the reply is a new channel which happens to reuse the request
   address, rather than the request channel itself.
