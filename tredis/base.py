"""
Base class

"""
from tornado import concurrent


class Category(object):
    """Base class extended by all classes that implement a category  of Redis
    commands.

    """
    def __init__(self, parent):
        super(Category, self).__init__()
        self.parent = parent

        # Assign all of the public methods of this class to the parent
        for method in [m for m in dir(self.__class__)
                       if not m.startswith('_')]:
            setattr(self.parent, method, getattr(self, method))

    def _execute(self, parts, callback=None):
        """Execute a Redis command.

        :param list parts: The list of command parts
        :param method callback: The optional method to invoke when complete
        :rtype: :py:class:`tornado.concurrent.Future`

        """
        return self.parent.execute(parts, callback)

    def _execute_with_bool_response(self, parts):
        """Execute a command returning a boolean based upon the response.

        :param list parts: The command parts
        :rtype: bool

        """
        future = concurrent.TracebackFuture()

        def on_response(response):
            exc = response.exception()
            if exc:
                future.set_exception(exc)
            else:
                future.set_result(response.result() == 1)

        self.parent.execute(parts, on_response)
        return future
