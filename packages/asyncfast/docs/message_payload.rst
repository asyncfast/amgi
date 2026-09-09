#################
 Message Payload
#################

In a decorated channel any argument that is not a channel parameter, or header is treated as the payload.

.. important::

   Only one payload argument is allowed, there is no attempt to merge multiple, instead an error will be raised

***********
 BaseModel
***********

You can declare a data model as a class that inherits from ``BaseModel``. This can then be used as the payload argument:

.. async-fast-example:: examples/payload_basemodel.py

******
 List
******

.. async-fast-example:: examples/payload_list.py

***********
 Dataclass
***********

.. async-fast-example:: examples/payload_dataclass.py

**********
 Built in
**********

.. async-fast-example:: examples/payload_builtin.py

******
 Avro
******

By default a payload is parsed as JSON. Annotating it with ``AvroPayload`` parses it as `Avro`_ binary instead, from a
schema derived from the same type hints:

.. async-fast-example:: examples/payload_avro.py

The schema is published in the generated AsyncAPI document as a `Multi Format Schema Object`_, so consumers in other
languages can be generated from it.

Avro support is provided by the separate ``asyncfast-avro`` package:

.. code::

   pip install asyncfast-avro==0.47.0

.. important::

   Payloads are encoded as plain Avro binary, not the Confluent wire format, so no schema registry is involved. The
   schema in the AsyncAPI document is both the writer and the reader schema.

Type mapping
============

Avro schemas are derived from the JSON schema pydantic generates, so any type usable as a JSON payload can be used as an
Avro one. Types without a direct Avro equivalent are carried as strings:

.. list-table::
   :header-rows: 1

   -  -  Python
      -  Avro

   -  -  ``int``
      -  ``long``

   -  -  ``float``
      -  ``double``

   -  -  ``bytes``
      -  ``bytes``

   -  -  ``UUID``
      -  ``string`` (``uuid``)

   -  -  ``datetime``
      -  ``long`` (``timestamp-micros``)

   -  -  ``date``
      -  ``int`` (``date``)

   -  -  ``time``
      -  ``long`` (``time-micros``)

   -  -  ``Decimal``, ``timedelta``
      -  ``string``

   -  -  ``Enum``
      -  ``enum``, or the value type where the values are not valid Avro names

   -  -  ``BaseModel``, dataclass, ``TypedDict``
      -  ``record``

   -  -  ``list``, ``set``, ``tuple``
      -  ``array``

   -  -  ``dict``
      -  ``map``, keys are always encoded as strings

.. _avro: https://avro.apache.org/

.. _multi format schema object: https://www.asyncapi.com/docs/reference/specification/v3.0.0#multiFormatSchemaObject
