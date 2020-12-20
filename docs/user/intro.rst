Introduction
============

Abide is a lightweight simple task scheduler. The aim is to make it as easy as possible to define a set of tasks that should run according to a schedule. Task scripts are held in a single directory, and their schedule is defined in a schedule.yaml directory in that directory. Abide can take care of automatic retrying tasks that fail, capturing output, and providing directories for scripts to output to.

Simple Example
--------------

Create a new directory and add a ``schedule.yaml`` file containing the following:

.. code:: yaml

    example_task:
      file: example_task.py
      schedule: * * * * *

Add a Python task file ``example_task.py`` containing the following:

.. code:: python

    for i in range(10):
        print("Hello", i)

Finally, run the abide server:

.. code:: shell

    $ python -m abide server

The task will be run every minute, and the output will be displayed on the console.

Comparison to other schedulers
------------------------------

cron
~~~~

Unlike cron, Abide tracks the tasks that it runs. That allows it to provide sensible behavior for retrying or alerting

airflow
~~~~~~~

Airflow supports running on windows. It doesn't support airflow's dependency structure.

Windows Task Scheduler
~~~~~~~~~~~~~~~~~~~~~~

