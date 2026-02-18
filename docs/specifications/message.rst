#############################
 Message AMGI Message Format
#############################

The Message AMGI sub-specification outlines how messages are sent, and received within AMGI.

It is deliberately designed to be agnostic where possible. Terminology is taken from AsyncAPI_ so as to follow their
agnosticism.

A simple implementation would be:

.. literalinclude:: message.py
   :lines: 4-

*********
 Message
*********

A message has a single message scope. Your application will be called once per message.

The message scope information passed in scope contains:

.. typeddict:: amgi_types.MessageScope
   :type: scope

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
