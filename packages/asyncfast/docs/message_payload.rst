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
