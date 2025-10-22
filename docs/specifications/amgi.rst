###############################################################
 AMGI (Asynchronous Messaging Gateway Interface) Specification
###############################################################

**********
 Abstract
**********

This document proposes a standard interface between messaging protocol servers (Kafka, AQMP, MQTT) and Python
applications, intended to allow for writing applications that can support multiple event protocols.

This base specification aims to establish a fixed set of APIs through which these servers interact and execute
application code.

***********
 Rationale
***********

The ASGI_ standard has proved itself in the world of webservers, webservers such as uvicorn_ have proved itself in
production, and frameworks such as FastAPI_ are among the top most started projects on GitHub_.

One of the most useful feature of ASGI is its portability, with Mangum_ allowing you to run ASGI compatible applications
within AWS Lambda.

AMGI aims to achieve this level of portability to support messaging protocols, it takes the lessons from ASGI and
attempts to have a similar simple application interface.

AMGI has two main goals:

-  Provide a way to write an application to handle messages, and without modification support multiple protocols.
-  Allow for a simple method of portability in terms of compute, you should be able to run your application anywhere,
   whether that be on a persistent server, or a serverless environment

**********
 Overview
**********

ASGI consists of two components:

-  A *protocol server*, consumes messages from the specific protocol, and sends any messages the .
-  An *application*, which lives inside a protocol server, this is called once per batch of messages (dependent on
   protocol), the application will emit events to send, and acknowledgements when a message is processed.

Like ASGI, applications are called, and awaited with a ``scope``, along with two awaitable callables: :py:func:`receive`
event messages, and :py:func:`send` event messages back. All this happening in an asynchronous event loop.

.. _asgi: https://asgi.readthedocs.io/en/latest/

.. _fastapi: https://fastapi.tiangolo.com/

.. _github: https://github.com/

.. _mangum: https://mangum.fastapiexpert.com/

.. _uvicorn: https://uvicorn.dev/
