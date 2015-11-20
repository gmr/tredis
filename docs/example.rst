Example
=======
The following examples expect a pre-existing asynchronous application:

.. code-block:: python
   :caption: A simple set and get of a key from Redis

   client = tredis.RedisClient()

   yield client.set('foo', 'bar')
   value = yield client.get('foo')

.. code-block:: python
   :caption: Pipeline Example
   :name: examples-pipeline

   client = tredis.RedisClient()

   # Start the pipeline
   client.pipeline_start()

   client.set('foo1', 'bar1')
   client.set('foo2', 'bar2')
   client.set('foo3', 'bar3')
   client.get('foo1')
   client.get('foo2')
   client.get('foo3')
   client.incr('foo4')
   client.incr('foo4')
   client.get('foo4')

   # Execute the pipeline
   responses = yield client.pipeline_execute()

   # The expected responses should match this list
   assert responses == [True, True, True, b'bar1', b'bar2', b'bar3', 1, 2, b'2']

.. warning:: Yielding after calling :py:meth:`pipeline_start <tredis.RedisClient.pipeline_start>`
   and before calling :py:meth:`pipeline_execute <tredis.RedisClient.pipeline_execute>`
   can cause asynchronous request scope issues, as the client does not protect against other
   asynchronous requests from populating the pipeline. The only way to prevent
   this from happening is to make all pipeline additions inline without yielding
   to the :py:class:`IOLoop <tornado.ioloop.IOLoop>`.

