###########
 Receiving
###########

Receiving messages is handled by the ``@app.channel(name)`` decorator.

.. async-fast-example:: examples/payload_basemodel.py

You can process and parse the payload, parameters, and headers of a
message.

.. toctree::
   :hidden:

   message_payload
   message_headers
   channel_parameters
