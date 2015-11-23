Version History
===============

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
