###########
 AsyncFast
###########

AsyncFast is a modern, event framework for building APIs with Python based on standard Python type hints.

Key Features:

-  **Portable**: Following AMGI should allow for implementations of any protocol
-  **Standards-based**: Based on AsyncAPI_ and `JSON Schema`_

*************
 Terminology
*************

Terminology throughout this documentation will follow AsyncAPI_ where possible, this means using terms like ``channel``
instead of ``topic`` etc.

******
 AMGI
******

AMGI_ (Asynchronous Messaging Gateway Interface) is the spiritual sibling of ASGI_. While the focus of ASGI_ is HTTP,
the focus here is event-based applications.

.. warning::

   Given this project is in the very early stages treat AMGI as unstable

**************
 Installation
**************

.. code::

   pip install "asyncfast[standard]"

*************
 Basic Usage
*************

The simplest AsyncFast could be:

.. async-fast-example:: examples/payload_basemodel.py

Running
=======

To run the app install an AMGI server (at the moment there is only ``amgi-aiokafka``) then run:

.. code::

   $ asyncfast run amgi-aiokafka main:app channel

AsyncAPI Generation
===================

.. code::

   $ asyncfast asyncapi main:app

**************
 Requirements
**************

This project stands on the shoulders of giants:

-  Pydantic_ for the data parts.

Taking ideas from:

-  FastAPI_ for the typed API parts.
-  ASGI_ for the portability.

.. toctree::
   :hidden:

   receiving
   sending
   lifespan

.. _amgi: https://amgi.readthedocs.io/en/latest/

.. _asgi: https://asgi.readthedocs.io/en/latest/

.. _asyncapi: https://www.asyncapi.com/

.. _fastapi: https://fastapi.tiangolo.com/

.. _json schema: https://json-schema.org/

.. _pydantic: https://docs.pydantic.dev/
