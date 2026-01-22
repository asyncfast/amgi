#############################
 Message AMGI Message Format
#############################

The Message AMGI sub-specification outlines how messages are sent, and received within AMGI.

It is deliberately designed to be agnostic where possible. Terminology is taken from AsyncAPI_ so as to follow their
agnosticism.

A simple implementation would be:

.. code:: python

    async def app(scope, receive, send):
        if scope["type"] == "message":
            more_messages = True
            while more_messages:
                message = await receive()
                message_id = message["id"]
                try:
                    headers = message["headers"]
                    payload = message.get("payload")
                    bindings = message.get("bindings", {})
                    ...  # Do some message handling here!
                    await send(
                        {
                            "type": "message.ack",
                            "id": message_id,
                        }
                    )
                except Exception as e:
                    await send(
                        {
                            "type": "message.nack",
                            "id": message_id,
                            "message": str(e),
                        }
                    )
                more_messages = message.get("more_messages")
        else:
            pass  # Handle other types


*********
 Message
*********

A message batch has a single message scope. Your application will be called once per batch. For protocols that do not
support batched consumption a batch of one message should be sent to the application.

The message scope information passed in scope contains:

.. typeddict:: amgi_types.MessageScope
   :type: scope

********************************************
 Receive message - :py:func:`receive` event
********************************************

Sent to the application to indicate an incoming message in the batch.

Keys:

.. typeddict:: amgi_types.MessageReceiveEvent

**********************************************
 Response message ack - :py:func:`send` event
**********************************************

Sent by the application to signify that it has successfully acknowledged a message.

Keys:

.. typeddict:: amgi_types.MessageAckEvent

***********************************************
 Response message nack - :py:func:`send` event
***********************************************

Sent by the application to signify that it could not process a message.

Keys:

.. typeddict:: amgi_types.MessageNackEvent

***********************************************
 Response message send - :py:func:`send` event
***********************************************

Sent by the application to send a message. If the server fails to send the message, the server should raise a
server-specific subclass of :py:obj:`OSError`.

Keys:

.. typeddict:: amgi_types.MessageSendEvent

*****************
 Bindings Object
*****************

Both ``"message.receive"``, and ``"message.send"`` can contain a bindings object. These are defined as per protocol
specifications.

.. toctree::

   bindings

.. _asyncapi: https://www.asyncapi.com/en
