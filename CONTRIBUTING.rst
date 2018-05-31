Local Development and Contributing
----------------------------------
The development environment for tredis uses `docker-compose <https://docs.docker.com/compose/>`_
and `docker-machine <https://docs.docker.com/machine/>`_

To get setup in the environment and run the tests, take the following steps:

.. code:: bash

    virtualenv -p python3 env
    source env/bin/activate

    ./bootstrap
    source build/test-environment

    nosetests

Please format your code contributions with the ``yapf`` formatter:

.. code:: bash

    yapf -i --style=pep8 tredis/MODULE.py

Pull requests that make changes or additions that are not covered by tests
will likely be closed without review.
