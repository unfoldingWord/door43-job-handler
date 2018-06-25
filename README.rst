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

Satisfy basic depedencies:

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
