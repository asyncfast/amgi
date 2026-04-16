#####################
 Message Send Router
#####################

``MessageSendRouter`` helps AMGI servers route ``message.send`` events to different backends based on the event address.
It manages setup and teardown of the underlying send callables using async context managers. This is useful when you
need to send different messages to different brokers, or you want to keep a single server running while routing
outbound traffic by address.

*************
 Basic Usage
*************

The router is an async context manager. Create it, register routes, and then enter it when the server starts so the
message senders can establish connections:

.. async-fast-example:: examples/message_send_router.py

In the example above, one address is routed to Kafka, and another to SQS. The app itself stays the same; routing is
configured at the server boundary.

***********************
 Integrating With Run
***********************

AMGI servers expect a ``send`` callable. ``MessageSendRouter`` provides that callable when you enter it, so pass the
router instance to the server and let it manage resource lifetimes:

.. code:: python

   from amgi_aiokafka import run
   from asyncfast.message_send import MessageSendRouter

   message_send_router = MessageSendRouter()
   # add routes...

   run(
       app,
       "orders",
       message_send=message_send_router,
   )

When the server starts, it enters the router and uses the callable it yields. When the server shuts down, it exits the
router and closes all message senders cleanly.

*******************
 Address Patterns
*******************

Routes use the same pattern syntax as channel parameters, so ``priority.{id}`` will match addresses like
``priority.123``. Register routes before entering the router so they are included in its setup.

If you need a catch-all pattern, register a default route instead of a broad pattern; this keeps route matching
explicit and easier to reason about.

****************
 Default Route
****************

If you pass ``default=``, the router will use that send callable when no route matches. Without a default, you should
ensure every outgoing address has a route registered, otherwise the send will fail at runtime.

The default sender should be an async context manager just like the routed senders. This allows you to share connection
pools or client lifecycles with explicit cleanup on shutdown.
