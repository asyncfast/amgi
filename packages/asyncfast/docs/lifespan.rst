##########
 Lifespan
##########

You can define logic that runs before an application starts up, and run it only once, as well as logic that runs when
the application shuts down, and run it only once.

This code covers the lifespan of the application, this can be useful to setup resources such as database connections,
and gracefully shut them down.

Lifespan can be passed as a context manager to the application, with either side of the context manager being startup,
and shutdown.

This is an example below, which will cleanly close the Redis connection:

.. async-fast-example:: examples/lifespan.py
