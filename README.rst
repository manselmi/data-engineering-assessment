===========================
Data Engineering assessment
===========================

Part 1
======

Please see the Python script ``01.py`` and the comments throughout the
code.

Prerequisites
-------------

* Python 3.6+

Prepare the environment by running the following code:

::

    python -m venv venv
    source venv/bin/activate
    python -m pip install --upgrade pip setuptools wheel
    python -m pip install --no-deps -r requirements.txt
    python -m pip check

Invocation
----------

::

    ./01.py

Running this will create the ``01.parquet`` Parquet file.

Part 2
======

Please see the Python script ``02.py`` and the comments throughout the
code.

Prerequisites
-------------

* Docker

Download the necessary container image:

::

    docker image pull docker.io/bitnami/spark:latest

Invocation
----------

::

    docker container run \
      --mount type=bind,source="$(pwd)",target=/mnt/workspace \
      --rm \
      --tty \
      docker.io/bitnami/spark:latest \
      spark-submit /mnt/workspace/02.py

Running this will create the ``02.parquet`` Parquet file.
