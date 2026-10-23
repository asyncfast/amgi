###############
 Request/Reply
###############

Request/reply is where a requester sends a message and waits for a response, with the response sent back to the
requester. In AsyncFast the reply is the message returned by the channel handler, so the pattern is declared by
annotating the return type of the handler with :py:class:`Reply`.

*************
 Basic Usage
*************

Annotating the return type with :py:class:`Reply` sends the returned message, and adds the AsyncAPI Operation Reply
object to the receive operation, where the reply channel is the channel of the returned message:

.. async-fast-example:: examples/request_reply.py

The return type must be a single :py:class:`Message` type, otherwise an :py:class:`InvalidChannelDefinitionError` is
raised when the channel is registered, that is, when the ``@app.channel(...)`` decorator is applied. Returning
``None`` sends no reply. :py:class:`Reply` can't be used on a generator handler, since a generator yields messages
rather than returning one.

***********************
 Dynamic Reply Address
***********************

When the address of the reply isn't known at design time, it is determined at runtime from the request itself. This
is typically done by the requester including a ``replyTo`` header. Passing that :py:class:`Header` as the address of
the :py:class:`Reply` sends the returned message to the address in the header, so the handler doesn't need the header
in its signature:

.. async-fast-example:: examples/request_reply_dynamic_address.py

The header must be given an alias, which is the name of the header in the request. The header is added to the
request message, and the AsyncAPI reply address is the runtime expression ``$message.header#/replyTo``. A
``description`` given to the :py:class:`Header` becomes the description of the reply address.

The reply message must be declared with ``address=None``, since its address only exists at runtime. AsyncAPI 3.0
requires that, when a reply address is specified, the address of the channel referenced by the reply is ``null``,
which is how the reply channel is rendered.

********************
 Same Reply Address
********************

The request and the reply can also share an address, which is a reply channel using the same address as the request
channel:

.. code-block:: python

   @dataclass
   class Pong(Message, address="ping"):
       value: str


   @app.channel("ping")
   async def ping(payload: Ping) -> Annotated[Pong, Reply()]:
       return Pong(value=payload.value)

.. note::

   AsyncFast generates a channel per message sent, so the reply is a new channel which happens to reuse the request
   address, rather than the request channel itself.
