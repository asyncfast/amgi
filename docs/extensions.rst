############
 Extensions
############

The ASGI specification provides for server-specific extensions to be used outside of the core ASGI specification. This
document specifies some common extensions.

******************************
 Acknowledgement Out Of Order
******************************

This is sent by the server to indicate that acknowledgements can be sent out of order.

.. code::

   "scope": {
       ...
       "extensions": {
           "message.ack.out_of_order": {}
       }
   }

When this extension is present, the application MAY send ``message.ack`` and ``message.nack`` events for received
messages in any order. The server MUST accept out-of-order acknowledgements and MUST NOT treat them as a protocol error.

When this extension is absent, applications SHOULD assume that acknowledgement ordering is constrained by the server and
MAY be required to follow message delivery order.

This extension does not change the semantics of message delivery or batching, and only affects acknowledgement ordering.
