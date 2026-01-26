###################
 Header Parameters
###################

Use ``Header()`` to declare that a parameter should be read from message headers.

AsyncFast header handling follows the `AsyncAPI specification
<https://www.asyncapi.com/docs/reference/specification/v3.0.0#specification>`_ for message headers and schema and is
therefore case sensitive

By default, the header name is derived from the argument name and underscores become hyphens, for example, the argument
``request_id`` would become ``request-id``

You can also set an explicit header key using ``alias=`` (useful for exact casing like ``Idempotency-Key`` or ``ETag``).
These aliases are reflected both at runtime and in the generated AsyncAPI schema.

.. async-fast-example:: examples/header_builtin.py

*****************
 Header Aliasing
*****************

Use ``alias=`` when you need a specific header name (including casing):

.. async-fast-example:: examples/header_alias.py

*********
 Default
*********

Header parameters can have defaults, just like normal arguments. If the header is not present, the default value is
used.

.. async-fast-example:: examples/header_default.py
