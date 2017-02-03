Example
=======
The following examples expect a pre-existing asynchronous application:

.. code-block:: python
   :caption: A simple set and get of a key from Redis

    import logging
    import pprint

    from tornado import gen, ioloop
    import tredis


    @gen.engine
    def run():
        client = tredis.Client([{"host": "127.0.0.1", "port": 6379, "db": 0}],
                               auto_connect=False)
        yield client.connect()
        yield client.set("foo", "bar")
        value = client.get("foo")
        pprint.pprint(value)
        ioloop.IOLoop.current().stop()

    if __name__ == '__main__':
        logging.basicConfig(level=logging.DEBUG)
        io_loop = ioloop.IOLoop.current()
        io_loop.add_callback(run)
        io_loop.start()
