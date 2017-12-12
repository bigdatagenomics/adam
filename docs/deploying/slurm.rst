Running ADAM on Slurm
---------------------

For those groups with access to a HPC cluster with
`Slurm <https://en.wikipedia.org/wiki/Slurm_Workload_Manager>`__
managing a number of compute nodes with local and/or network attached
storage, it is possible to spin up a temporary Spark cluster for use by
ADAM.

The full IO bandwidth benefits of Spark processing are likely best
realized through a set of co-located compute/storage nodes. However,
depending on your network setup, you may find Spark deployed on HPC to
be a workable solution for testing or even production at scale,
especially for those applications which perform multiple in-memory
transformations and thus benefit from Spark's in-memory processing
model.

Follow the primary
`instructions <https://github.com/bigdatagenomics/adam/blob/master/docs/source/02_installation.md>`__
for installing ADAM into ``$ADAM_HOME``. This will most likely be at a
location on a shared disk accessible to all nodes, but could be at a
consistant location on each machine.

Start Spark cluster
~~~~~~~~~~~~~~~~~~~

A Spark cluster can be started as a multi-node job in Slurm by creating
a job file ``run.cmd`` such as below:

.. code:: bash

    #!/bin/bash

    #SBATCH --partition=multinode
    #SBATCH --job-name=spark-multi-node
    #SBATCH --exclusive

    #Number of seperate nodes reserved for Spark cluster
    #SBATCH --nodes=2
    #SBATCH --cpus-per-task=12

    #Number of excecution slots
    #SBATCH --ntasks=2

    #SBATCH --time=05:00:00
    #SBATCH --mem=248g

    # If your sys admin has installed spark as a module
    module load spark

    # If Spark is not installed as a module, you will need to specifiy absolute path to 
    # $SPARK_HOME/bin/spark-start where $SPARK_HOME is on shared disk or at a consistant location
    start-spark

    echo $MASTER
    sleep infinity

Submit the job file to Slurm:

::

    sbatch run.cmd

This will start a Spark cluster containing two nodes that persists for
five hours, unless you kill it sooner. The file ``slurm.out`` created in
the current directory will contain a line produced by ``echo $MASTER``
above which will indicate the address of the Spark master to which your
application or ADAM-shell should connect such as
``spark://somehostname:7077``

Start adam-shell
~~~~~~~~~~~~~~~~

Your sys admin will probably prefer that you launch your ``adam-shell``
or start an application from a cluster node rather than the head node
you log in to. You may want to do so with:

::

    sinteractive

Start an adam-shell:

::

    $ADAM_HOME/bin/adam-shell --master spark://hostnamefromslurmdotout:7077

or Run a batch job with adam-submit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    $ADAM_HOME/bin/adam-submit --master spark://hostnamefromslurmdotout:7077

You should be able to connect to the Spark Web UI at
``http://hostnamefromslurmdotout:4040``, however you may need to ask
your system administrator to open the required ports.

