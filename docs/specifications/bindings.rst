########################
 Bindings specification
########################

Are for protocol specific message properties, they must match the naming conventions of the `Message Bindings Object`_
in AsyncAPI_.

A message sent to an application can only contain one protocol in its binding object. A message sent from an application
can include multiple protocol bindings. The server simply ignores the binding if it cannot handle it. This is to allow
for progressive migrations of an application

*******
 Kafka
*******

The Kafka binding object has the following key:

-  ``message["bindings"]["keys"]`` (:py:obj:`~typing.NotRequired` [ :py:obj:`~typing.Optional` [:py:obj:`bytes` ]]) -
   The :py:obj:`bytes` of the Kafka key, this can be also be :py:obj:`None`. If missing, it defaults to :py:obj:`None`.

********
 STOMP
********

The STOMP binding object has the following keys:

-  ``message["bindings"]["stomp"]["message_id"]`` - The :py:obj:`str` message id of the STOMP message, assigned by the
   broker. Useful for deduplication.
-  ``message["bindings"]["stomp"]["subscription"]`` - The :py:obj:`str` id of the STOMP subscription the message was
   received on.

.. _asyncapi: https://www.asyncapi.com/en

.. _message bindings object: https://www.asyncapi.com/docs/reference/specification/v3.0.0#messageBindingsObject
