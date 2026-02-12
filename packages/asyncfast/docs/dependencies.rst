##############
 Dependencies
##############

Dependencies let you share common logic across channel handlers. They are declared with ``Depends`` and injected using
``typing.Annotated``.

A dependency function can be ``def`` or ``async def``. It can also be a generator (sync or async) to provide setup and
cleanup logic.

******************
 Basic Dependency
******************

Use ``Depends`` inside ``Annotated`` to tell AsyncFast to resolve a value and pass it to your handler:

.. async-fast-example:: examples/dependency_basic.py

**********************************
 Dependencies Use The Same Inputs
**********************************

Dependency functions can declare the same kinds of parameters as a channel handler:

-  ``Payload`` (the message body)
-  ``Header`` values
-  channel parameters (from the address template)
-  bindings (via protocol binding types)
-  ``MessageSender`` for sending follow-up messages

These inputs are resolved for dependencies exactly the same way they are for handlers.

******************
 Sub-dependencies
******************

Dependencies can depend on other dependencies using the same ``Depends`` pattern:

.. async-fast-example:: examples/dependency_sub_dependency.py

********************
 Cleanup With Yield
********************

If a dependency is a generator, AsyncFast treats it like a context manager and runs cleanup after the handler returns.
This works for both ``def`` generators and ``async def`` generators:

.. async-fast-example:: examples/dependency_yield.py

*********
 Caching
*********

By default, a dependency result is cached per message and reused if requested multiple times. To disable caching, set
``use_cache=False`` on ``Depends``:

.. async-fast-example:: examples/dependency_cache.py
