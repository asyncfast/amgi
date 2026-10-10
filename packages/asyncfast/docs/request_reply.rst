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
is typically done by the requester including a ``replyTo`` header, whose location is given to
:py:class:`ReplyAddress`:

.. async-fast-example:: examples/request_reply_dynamic_address.py

The location must follow the AsyncAPI runtime expression format, where either ``$message.header`` or
``$message.payload`` is followed by a JSON Pointer, for example ``"$message.header#/replyTo"``. As a shortcut, a
location can also be passed to :py:class:`Reply` directly as a string:

.. code-block:: python

   @app.channel("ping", reply=Reply(address="$message.header#/replyTo"))
   async def ping(payload: str) -> AsyncGenerator[Pong, None]:
       yield Pong(payload="pong")

The reply address is documentation only, so the handler is still responsible for sending to the address the requester
supplied. Binding the header to a parameter of the reply channel address, as in the example above, is the most direct
way of doing this.

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
