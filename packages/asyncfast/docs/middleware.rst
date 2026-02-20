############
 Middleware
############

Middleware lets you wrap the AMGI application to run logic before, or after a message is handled. A middleware is a
callable that receives the downstream app, and is itself an AMGI application.

Middleware can be used for cross-cutting concerns like logging, timing, tracing, metrics, or translating errors. It sees
the full AMGI ``scope``, and the ``receive``/``send`` callables for each event.

**********************
 What Middleware Runs
**********************

AsyncFast builds a middleware stack that wraps the core app. Each middleware is called for every AMGI event handled by
the app:

-  ``message`` scopes for regular channel handling.
-  ``lifespan`` scopes for startup, and shutdown.

If you need one-time startup, or shutdown logic, prefer the lifespan API. Middleware is for per-event behavior.

******************
 Basic Middleware
******************

Create a class with ``__init__`` to receive the downstream app, and ``__call__`` to handle each event:

.. async-fast-example:: examples/middleware_basic.py

The code before ``await self._app(...)`` runs before the handler (and its dependencies). The code after runs after the
handler finishes. This is the standard pattern for timing, logging, or error handling.

If you add dependencies that use ``yield`` for cleanup, their teardown runs inside the downstream app, so it completes
before the code after ``await self._app(...)``.

*********************************
 Working With Scope And Messages
*********************************

The middleware callable receives:

-  ``scope``: a dict describing the AMGI event (including ``type``, channel address, headers, and protocol info).
-  ``receive``: an async callable that yields inbound events.
-  ``send``: an async callable used to emit outbound events.

Most middleware simply passes these through to the downstream app. If you need to inspect, or transform traffic, you can
wrap ``receive`` or ``send`` before passing them along. When you do, make sure you preserve the expected event flow, and
always ``await`` the downstream app exactly once.

************************
 Registering Middleware
************************

You can register middleware when creating the app:

.. code:: python

   from asyncfast import AsyncFast
   from asyncfast import Middleware

   app = AsyncFast(
       middleware=[Middleware(MyMiddleware, "arg1", option=True)],
   )

Or add it later:

.. code:: python

   app = AsyncFast()
   app.add_middleware(MyMiddleware, "arg1", option=True)

.. note::

   The middleware stack is built on first use. Add middleware before the app starts handling messages.

******************
 Middleware Order
******************

Middleware wraps the app in the order it is registered. The last middleware added runs first.

For example, if you add ``FirstMiddleware``, and then ``SecondMiddleware``, the call order is:

#. ``SecondMiddleware`` before
#. ``FirstMiddleware`` before
#. router
#. ``FirstMiddleware`` after
#. ``SecondMiddleware`` after
