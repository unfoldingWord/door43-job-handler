master:

.. image:: https://travis-ci.org/unfoldingWord-dev/door43-job-handler.svg?branch=master
    :alt: Build Status
    :target: https://travis-ci.org/unfoldingWord-dev/door43-job-handler?branch=master

.. image:: https://coveralls.io/repos/github/unfoldingWord-dev/door43-job-handler/badge.svg?branch=master
    :alt: Coveralls
    :target: https://coveralls.io/github/unfoldingWord-dev/door43-job-handler?branch=master

develop:

.. image:: https://travis-ci.org/unfoldingWord-dev/door43-job-handler.svg?branch=develop
    :alt: Build Status
    :target: https://travis-ci.org/unfoldingWord-dev/door43-job-handler?branch=develop

.. image:: https://coveralls.io/repos/github/unfoldingWord-dev/door43-job-handler/badge.svg?branch=develop
    :alt: Coveralls
    :target: https://coveralls.io/github/unfoldingWord-dev/door43-job-handler?branch=develop


door43-job-handler (part of tx platform)
========================================

This program accepts jobs from a rq/redis queue (placed there by the
[door43-enqueue-job](https://github.com/unfoldingWord-dev/door43-enqueue-job)) program.

Setup
-----

Requires Python 3.6 (Python2 compatibility has been removed.)

Satisfy basic dependencies:

.. code-block:: bash

    git clone https://github.com/unfoldingWord-dev/door43-job-handler.git
    OR/ with ssh: git clone git@github.com:unfoldingWord-dev/door43-job-handler.git
    sudo apt-get install python3-pip

We recommend you create a Python virtual environment to help manage Python package dependencies:

.. code-block:: bash

    cd door43-job-handler
    python3 -m venv venv

Now load that virtual environment and install dependencies:

.. code-block:: bash

    source venv/bin/activate
    make dependencies

Deploymemt
----------

Travis-CI is hooked to from GitHub to automatically test commits to both the `develop`
and `master` branches, and on success, to build containers (tagged with those branch names)
that are pushed to [DockerHub](https://hub.docker.com/u/unfoldingword/).

To test the container (assuming that the confidential environment variables are already set in the current environment) use:
.. code-block:: bash
 	docker run --env TX_DATABASE_PW --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --env QUEUE_PREFIX=dev- --env DEBUG_MODE=True --env REDIS_URL=<redis_url> --net="host" --name door43_job_handler --rm door43_job_handler


To run the container in production use with the desired values:
.. code-block:: bash
     	docker run --env TX_DATABASE_PW=<tx_db_pw> --env AWS_ACCESS_KEY_ID=<access_key> --env AWS_SECRET_ACCESS_KEY=<sa_key> --env GRAPHITE_URL=<graphite_url> --env REDIS_URL=<redis_url> --net="host" --name door43_job_handler --rm door43_job_handler

The production container will be deployed to the unfoldingWord AWS EC2 instance, where
[Watchtower](https://github.com/v2tec/watchtower) will automatically check for, pull, and run updated containers.
