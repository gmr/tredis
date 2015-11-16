Example
=======

.. code:: python

   client = tredis.RedisClient()
   yield client.connect()

   yield client.set('foo', 'bar')
   value = yield client.get('foo')



