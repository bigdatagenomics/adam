Running ADAM on CDH 5, HDP, and other YARN based Distros
--------------------------------------------------------

`Apache Hadoop
YARN <http://hadoop.apache.org/docs/stable2/hadoop-yarn/hadoop-yarn-site/YARN.html>`__
is a widely used scheduler in the Hadoop ecosystem. YARN stands for "Yet
Another Resource Negotiator", and the YARN architecture is described in
(Vavilapalli et al. 2013). YARN is used in several common Hadoop
distributions, including the `Cloudera Hadoop Distribution
(CDH) <http://www.cloudera.com/products/apache-hadoop/key-cdh-components.html>`__
and the `Hortonworks Data Platform
(HDP) <http://hortonworks.com/products/data-center/hdp/>`__. YARN is
supported natively in
`Spark <http://spark.apache.org/docs/latest/running-on-yarn.html>`__.

The ADAM CLI and shell can both be run on YARN. The ADAM CLI can be run
in both Spark's YARN ``cluster`` and ``client`` modes, while the ADAM
shell can only be run in ``client`` mode. In the ``cluster`` mode, the
Spark driver runs in the YARN ``ApplicationMaster`` container. In the
``client`` mode, the Spark driver runs in the submitting process. Since
the Spark driver for the Spark/ADAM shell takes input on stdin, it
cannot run in ``cluster`` mode.

To run the ADAM CLI in YARN ``cluster`` mode, run the following command:

::

    adam-submit \
      --master yarn \
      --deploy-mode cluster \
      -- \
      <adam_command_name> [options] \

In the ``adam-submit`` command, all options before the ``--`` are passed
to the ``spark-submit`` script, which launches the Spark job. To run in
``client`` mode, we simply change the ``deploy-mode`` to ``client``:

::

    adam-submit \
      --master yarn \
      --deploy-mode client \
      -- \
      <adam_command_name> [options] \

In the ``adam-shell`` command, all of the arguments are passed to the
``spark-shell`` command. Thus, to run the ``adam-shell`` on YARN, we
run:

::

    adam-shell \
      --master yarn \
      --deploy-mode client

All of these commands assume that the Spark assembly you are using is
properly configured for your YARN deployment. Typically, if your Spark
assembly is properly configured to use YARN, there will be a symbolic
link at ``${SPARK_HOME}/conf/yarn-conf/`` that points to the core
Hadoop/YARN configuration. Though, this may vary by the distribution you
are running.

The full list of configuration options for running Spark-on-YARN can be
found
`online <http://spark.apache.org/docs/latest/running-on-yarn.html#configuration>`__.
Most of the standard configurations are consistent between Spark
Standalone and Spark-on-YARN. One important configuration option to be
aware of is the YARN memory overhead parameter. From 1.5.0 onwards,
Spark makes aggressive use of off-heap memory allocation in the JVM.
These allocations may cause the amount of memory taken up by a single
executor (or, theoretically, the driver) to exceed the
``--driver-memory``/``--executor-memory`` parameters. These parameters
are what Spark provides as a memory resource request to YARN. By
default, if one of your Spark containers (an executors or the driver)
exceeds its memory request, YARN will kill the container by sending a
``SIGTERM``. This can cause jobs to fail. To eliminate this issue, you
can set the ``spark.yarn.<role>.memoryOverhead`` parameter, where
``<role>`` is one of ``driver`` or ``executor``. This parameter is used
by Spark to increase its resource request to YARN over the JVM Heap size
indicated by ``--driver-memory`` or ``--executor-memory``.

As a final example, to run the ADAM
`transformAlignments <#transformAlignments>`__ CLI using YARN cluster
mode on a 64 node cluster with one executor per node and a 2GB per
executor overhead, we would run:

::

    adam-submit \
      --master yarn \
      --deploy-mode cluster \
      --driver-memory 200g \
      --executor-memory 200g \
      --conf spark.driver.cores=16 \
      --conf spark.executor.cores=16 \
      --conf spark.yarn.executor.memoryOverhead=2048 \
      --conf spark.executor.instances=64 \
      -- \
      transformAlignments in.sam out.adam

In this example, we are allocating 200GB of JVM heap space per executor
and for the driver, and we are telling Spark to request 16 cores per
executor and for the driver.
