Version History
===============

- 0.7.0
  - Adds support for Redis Clusters in the new :class:`~tredis.Client` class.

- 0.6.0 - released *2017-01-27*
  - Add :class:`~tredis.Client.zrem` to the `Sorted Sets <http://redis.io/commands#sorted_set>`_ commands
  - Locate master and reconnect when a ``READONLY`` response is received
  - Add :class:`~tredis.Client.time` command

- 0.5.0 - released *2016-11-08*

  - Add `Hash <http://redis.io/commands#hash>`_ commands (13 of 15)
  - Add `Sorted Sets <http://redis.io/commands#sorted_set>`_ commands (3 of 21)

- 0.4.0 - released *2016-01-25*

  - Add :class:`~tredis.Client.info` command

- 0.3.0 - released *2016-01-18*

  - Remove broken pipelining implementation
  - Add scripting commands

- 0.2.1 - released *2015-11-23*

  - Add hiredis to the requirements

- 0.2.0 - released *2015-11-23*

  - Add per-command execution locking, preventing errors with concurrency in command processing
    - Clean up connection logic to simplify connecting to exist within the command execution lock instead of maintaining its own event
  - Add all missing methods in the strings category
  - Add hyperloglog methods
  - Add support for mixins to extend core tredis.RedisClient methods in future versions
  - Significant updates to docstrings

- 0.1.0 - released *2015-11-20*

  - initial version
