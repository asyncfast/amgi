#########
 Testing
#########

Use pytest-amgi_ to drive an AsyncFast app in-process from a test. The
``amgi_producer`` fixture starts the AMGI lifespan protocol before returning a
producer, and shuts lifespan down when the test finishes.

.. code:: bash

   pip install pytest-amgi

The examples below use an app defined in ``app.py`` and tests in
``test_app.py``.

*************
 Basic Test
*************

Send a message with :py:meth:`AMGIProducer.send` and assert the app acked it:

.. literalinclude:: examples/testing_app.py
   :pyobject: orders

.. literalinclude:: examples/testing_pytest_amgi.py
   :pyobject: test_message

*********
 Headers
*********

Headers can be sent as a mapping. AsyncFast can read them with
:py:class:`Header`.

.. literalinclude:: examples/testing_app.py
   :pyobject: orders_with_header

.. literalinclude:: examples/testing_pytest_amgi.py
   :pyobject: test_message_header

***************
 Message Sends
***************

If a handler sends follow-up messages with :py:class:`MessageSender`, assert
them with :py:meth:`AMGIMessageResult.assert_has_message_send`.

.. literalinclude:: examples/testing_app.py
   :start-after: # process-order-start
   :end-at: await message_sender.send(ProcessOrder(payload={"id": 1}))


.. literalinclude:: examples/testing_pytest_amgi.py
   :pyobject: test_message_send

For multiple follow-up messages, use
:py:meth:`AMGIMessageResult.assert_has_message_sends`. This checks the number
and order of messages sent by the app.

.. literalinclude:: examples/testing_app.py
   :start-after: # multiple-sends-start
   :end-at: await message_sender.send(SendReceipt(payload={"id": 1}))

.. literalinclude:: examples/testing_pytest_amgi.py
   :pyobject: test_multiple_message_sends

*******
 Nacks
*******

Use :py:meth:`AMGIMessageResult.assert_nacked` when the app rejects a message.

.. literalinclude:: examples/testing_app.py
   :pyobject: orders_invalid

.. literalinclude:: examples/testing_pytest_amgi.py
   :pyobject: test_message_nack

.. _pytest-amgi: https://pypi.org/project/pytest-amgi/
