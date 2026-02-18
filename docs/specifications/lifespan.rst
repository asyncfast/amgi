###################
 Lifespan Protocol
###################

The Message AMGI lifespan sub-specification outlines how applications are started up and shut down within AMGI.

It follows the same semantics and event flow as the ASGI lifespan specification. This allows servers and applications to
coordinate resource allocation, background tasks, and graceful shutdown.

A simple implementation would be:


.. literalinclude:: lifespan.py
   :lines: 4-

**********
 Lifespan
**********

A lifespan session has a single lifespan scope. Your application will be called exactly once for the duration of the
process lifetime.

The lifespan scope information passed in ``scope`` contains:

.. typeddict:: amgi_types.LifespanScope
   :type: scope

*********************************************
 Lifespan startup - :py:func:`receive` event
*********************************************

Sent to the application to indicate that the server is starting up.

Applications should perform any required initialization at this point (for example: opening connections, warming caches,
or starting background tasks).

Keys:

.. typeddict:: amgi_types.LifespanStartupEvent

***************************************************
 Lifespan startup complete - :py:func:`send` event
***************************************************

Sent by the application to signify that startup has completed successfully.

Once this event is sent, the server may begin delivering messages to the application.

Keys:

.. typeddict:: amgi_types.LifespanStartupCompleteEvent

*************************************************
 Lifespan startup failed - :py:func:`send` event
*************************************************

Sent by the application to signify that startup has failed.

If this event is sent, the server must not start message processing and should terminate the application.

Keys:

.. typeddict:: amgi_types.LifespanStartupFailedEvent

**********************************************
 Lifespan shutdown - :py:func:`receive` event
**********************************************

Sent to the application to indicate that the server is shutting down.

Applications should begin graceful shutdown at this point (for example: stopping background tasks, closing connections,
and flushing buffers).

Keys:

.. typeddict:: amgi_types.LifespanShutdownEvent

****************************************************
 Lifespan shutdown complete - :py:func:`send` event
****************************************************

Sent by the application to signify that shutdown has completed successfully.

Once this event is sent, the server may safely terminate the application.

Keys:

.. typeddict:: amgi_types.LifespanShutdownCompleteEvent

**************************************************
 Lifespan shutdown failed - :py:func:`send` event
**************************************************

Sent by the application to signify that shutdown has failed.

The server should still terminate the application, but may log or surface the failure as appropriate.

Keys:

.. typeddict:: amgi_types.LifespanShutdownFailedEvent
