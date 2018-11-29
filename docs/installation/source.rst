Building ADAM from Source
=========================

You will need to have `Apache Maven <http://maven.apache.org/>`__
version 3.1.1 or later installed in order to build ADAM.

    **Note:** The default configuration is for Hadoop 2.7.3. If building
    against a different version of Hadoop, please pass
    ``-Dhadoop.version=<HADOOP_VERSION>`` to the Maven command.

.. code:: bash

    git clone https://github.com/bigdatagenomics/adam.git
    cd adam
    mvn install


Running ADAM
------------

ADAM is packaged as an
`überjar <https://maven.apache.org/plugins/maven-shade-plugin/>`__ and
includes all necessary dependencies, except for Apache Hadoop and Apache
Spark.

You might want to add the following to your ``.bashrc`` to make running
ADAM easier:

.. code:: bash

    alias adam-submit="${ADAM_HOME}/bin/adam-submit"
    alias adam-shell="${ADAM_HOME}/bin/adam-shell"

``$ADAM_HOME`` should be the path to where you have checked ADAM out on
your local filesystem. The first alias should be used for running ADAM
jobs that operate locally. The latter two aliases call scripts that wrap
the ``spark-submit`` and ``spark-shell`` commands to set up ADAM. You
will need to have the Spark binaries on your system; prebuilt binaries
can be downloaded from the `Spark
website <http://spark.apache.org/downloads.html>`__.

Once this alias is in place, you can run ADAM by simply typing
``adam-submit`` at the command line.

.. code:: bash

    adam-submit

Building for Python
-------------------

ADAM can be installed using the Pip package manager, or from source.
To build and test `ADAM's Python bindings <#python>`__, enable the
``python`` profile:

.. code:: bash

    mvn -P python package

This will enable the ``adam-python`` module as part of the ADAM build.
This module uses Maven to invoke a Makefile that builds a Python egg and
runs tests. To build this module, we require either an active
`Conda <https://conda.io/>`__ or
`virtualenv <https://virtualenv.pypa.io/en/stable/>`__ environment.

ADAM can run on both Python 2 and Python 3.
`To setup and activate a Conda
environment <https://conda.io/docs/using/envs.html>`__ for Python 2.7, run:

.. code:: bash

    conda create -n adam python=2.7 anaconda
    source activate adam

`To setup and activate a Conda
environment <https://conda.io/docs/using/envs.html>`__ for Python 3.6, run:

.. code:: bash

    conda create -n adam python=3.6 anaconda
    source activate adam


`To setup and activate a virtualenv
environment <https://virtualenv.pypa.io/en/stable/userguide/#usage>`__,
run:

.. code:: bash

    virtualenv adam
    . adam/bin/activate

Additionally, to run tests, the PySpark dependencies must be on the
Python module load path and the ADAM JARs must be built and provided to
PySpark. This can be done with the following bash commands:

.. code:: bash

    # add pyspark to the python path
    PY4J_ZIP="$(ls -1 "${SPARK_HOME}/python/lib" | grep py4j)"
    export PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/${PY4J_ZIP}:${PYTHONPATH}

    # put adam jar on the pyspark path
    ASSEMBLY_DIR="${ADAM_HOME}/adam-assembly/target"
    ASSEMBLY_JAR="$(ls -1 "$ASSEMBLY_DIR" | grep "^adam[0-9A-Za-z\.\_-]*\.jar$" | grep -v -e javadoc -e sources || true)"
    export PYSPARK_SUBMIT_ARGS="--jars ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} --driver-class-path ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} pyspark-shell"

This assumes that the `ADAM JARs have already been
built <#build-from-source>`__. Additionally, we require
`pytest <https://docs.pytest.org/en/latest/>`__ to be installed. The
adam-python makefile can install this dependency. Once you have an
active virtualenv or Conda environment, run:

.. code:: bash

    cd adam-python
    make prepare

Building for R
--------------

ADAM supports SparkR, for Spark 2.1.0 and onwards. To build and test
`ADAM's R bindings <#r>`__, enable the ``r`` profile:

.. code:: bash

    mvn -P r package

This will enable the ``adam-r`` module as part of the ADAM build. This
module uses Maven to invoke the ``R`` executable to build the
``bdg.adam`` package and run tests. The build requires the ``testthat``,
``devtools`` and ``roxygen`` packages

.. code:: bash

    R -e "install.packages('testthat', repos='http://cran.rstudio.com/')"
    R -e "install.packages('roxygen2', repos='http://cran.rstudio.com/')"
    R -e "install.packages('devtools', repos='http://cran.rstudio.com/')"

Installation of ``devtools`` may require ``libgit2`` as a dependency.

.. code:: bash

    apt-get install libgit2-dev

The build also requires you to have the ``SparkR`` package installed,
where ``v2.x.x`` should match your Spark version.

.. code:: bash

   R -e "devtools::install_github('apache/spark@v2.x.x', subdir='R/pkg')"

The ADAM JARs can then be provided to ``SparkR`` with the following bash
commands:

.. code:: bash

    # put adam jar on the SparkR path
    ASSEMBLY_DIR="${ADAM_HOME}/adam-assembly/target"
    ASSEMBLY_JAR="$(ls -1 "$ASSEMBLY_DIR" | grep "^adam[0-9A-Za-z\_\.-]*\.jar$" | grep -v javadoc | grep -v sources || true)"
    export SPARKR_SUBMIT_ARGS="--jars ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} --driver-class-path ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} sparkr-shell"

Note that the ``ASSEMBLY_DIR`` and ``ASSEMBLY_JAR`` lines are the same
as for the `Python build <#python-build>`__. As with the Python build,
this assumes that the `ADAM JARs have already been
built <#build-from-source>`__.
