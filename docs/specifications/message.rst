#############################
 Message AMGI Message Format
#############################

The Message AMGI sub-specification outlines how messages are sent, and received within AMGI.

It is deliberately designed to be agnostic where possible. Terminology is taken from AsyncAPI_ so as to follow their
agnosticism.

*********
 Message
*********

A message batch has a single message scope. Your application will be called once per batch. For protocols that do not
support batched consumption a batch of one message should be sent to the application.

The message scope information passed in scope contains:

-  ``scope["type"]`` (:py:obj:`str`) - ``"message"``
-  ``scope["amgi"]["version"]`` (:py:obj:`str`) - Version of the AMGI spec.
-  ``scope["amgi"]["spec_version"]`` (:py:obj:`str`) - Version of the AMGI message spec this server understands.
-  ``scope["address"]`` (:py:obj:`str`) - The address of the batch of messages, for example, in Kafka this would be the
   topic.

********************************************
 Receive message - :py:func:`receive` event
********************************************

Sent to the application to indicate an incoming message in the batch.

Keys:

-  ``message["type"]`` (:py:obj:`str`) – ``"message.receive"``

-  ``message["id"]`` (:py:obj:`str`) - A unique id for the message, used to ack, or nack the message

-  ``message["headers"]`` (:py:obj:`~collections.abc.Iterable` [:py:obj:`tuple` [:py:obj:`bytes`, :py:obj:`bytes`]]) -
   Includes the headers of the message.

-  ``message["payload"]`` (:py:obj:`~typing.NotRequired` [ :py:obj:`~typing.Optional` [:py:obj:`bytes`]]) - Payload of
   the message, which can be :py:obj:`None` or :py:obj:`bytes`. If missing, it defaults to :py:obj:`None`.

-  ``message["bindings"]`` (:py:obj:`~typing.NotRequired` [:py:obj:`dict` [:py:obj:`str`, :py:obj:`dict` [:py:obj:`str`,
   :py:obj:`~typing.Any`]]]) - Protocol specific bindings, for example, when receiving a Kafka message the bindings
   could include the key: ``{"kafka": {"key": b"key"}}``.

-  ``message["more_messages"]`` (:py:obj:`~typing.NotRequired` [:py:obj:`bool`]) - Indicates there are more messages to
   process in the batch. The application should keep receiving until it receives :py:obj:`False`. If missing it defaults
   to :py:obj:`False`.

**********************************************
 Response message ack - :py:func:`send` event
**********************************************

Sent by the application to signify that it has successfully acknowledged a message.

Keys:

-  ``message["type"]`` (:py:obj:`str`) – ``"message.ack"``
-  ``message["id"]`` (:py:obj:`str`) - The unique id of the message

***********************************************
 Response message nack - :py:func:`send` event
***********************************************

Sent by the application to signify that it could not process a message.

Keys:

-  ``message["type"]`` (:py:obj:`str`) – ``"message.nack"``
-  ``message["id"]`` (:py:obj:`str`) - The unique id of the message
-  ``message["message"]`` (:py:obj:`str`) - A message indicating why the message could not be processed

***********************************************
 Response message send - :py:func:`send` event
***********************************************

Sent by the application to send a message. If the server fails to send the message, the server should raise a
server-specific subclass of :py:obj:`OSError`.

Keys:

-  ``message["type"]`` (:py:obj:`str`) – ``"message.send"``

-  ``message["address"]`` (:py:obj:`str`) – Address to send the message to

-  ``message["address"]`` (:py:obj:`str`) – Address to send the message to

-  ``message["headers"]`` (:py:obj:`~collections.abc.Iterable` [:py:obj:`tuple` [:py:obj:`bytes`, :py:obj:`bytes`]]) -
   Headers of the message

-  ``message["payload"]`` (:py:obj:`~typing.NotRequired` [ :py:obj:`~typing.Optional` [:py:obj:`bytes`]]) - Payload of
   the message, which can be :py:obj:`None`, or :py:obj:`bytes`. If missing, it defaults to :py:obj:`None`.

-  ``message["bindings"]`` (:py:obj:`~typing.NotRequired` [:py:obj:`dict` [:py:obj:`str`, :py:obj:`dict` [:py:obj:`str`,
   :py:obj:`~typing.Any`]]]) - Protocol specific bindings to send. This can be bindings for multiple protocols, allowing
   the server to decide to handle them, or ignore them.

*****************
 Bindings Object
*****************

Both ``"message.receive"``, and ``"message.send"`` can contain a bindings object. These are defined as per protocol
specifications.

.. toctree::

   bindings

.. _asyncapi: https://www.asyncapi.com/en
