# Building ADAM from Source {#build-from-source}

You will need to have [Apache Maven](http://maven.apache.org/) version 3.1.1 or later
installed in order to build ADAM.

> **Note:** The default configuration is for Hadoop 2.7.3. If building against a different
> version of Hadoop, please pass `-Dhadoop.version=<HADOOP_VERSION>` to the Maven command.
> ADAM will cross-build for both Spark 1.x and 2.x, but builds by default against Spark
> 1.6.3. To build for Spark 2, run the `./scripts/move_to_spark2.sh` script.

```bash
git clone https://github.com/bigdatagenomics/adam.git
cd adam
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
mvn clean package -DskipTests
```
Outputs
```
...
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 9.647s
[INFO] Finished at: Thu May 23 15:50:42 PDT 2013
[INFO] Final Memory: 19M/81M
[INFO] ------------------------------------------------------------------------
```

You might want to take a peek at the `scripts/jenkins-test` script and give it a run. It will fetch a mouse chromosome, encode it to ADAM
reads and pileups, run flagstat, etc. We use this script to test that ADAM is working correctly.

## Running ADAM

ADAM is packaged as an [überjar](https://maven.apache.org/plugins/maven-shade-plugin/) and includes all necessary
dependencies, except for Apache Hadoop and Apache Spark. 

You might want to add the following to your `.bashrc` to make running ADAM easier:

```bash
alias adam-submit="${ADAM_HOME}/bin/adam-submit"
alias adam-shell="${ADAM_HOME}/bin/adam-shell"
```

`$ADAM_HOME` should be the path to where you have checked ADAM out on your local filesystem. 
The first alias should be used for running ADAM jobs that operate locally. The latter two aliases 
call scripts that wrap the `spark-submit` and `spark-shell` commands to set up ADAM. You will need
to have the Spark binaries on your system; prebuilt binaries can be downloaded from the
[Spark website](http://spark.apache.org/downloads.html). Our [continuous integration setup](
https://amplab.cs.berkeley.edu/jenkins/job/ADAM/) builds ADAM against Spark versions 1.6.1 and 2.0.0,
Scala versions 2.10 and 2.11, and Hadoop versions 2.3.0 and 2.6.0.

Once this alias is in place, you can run ADAM by simply typing `adam-submit` at the command line.

```bash
adam-submit
```

## Building for Python {#python-build}

To build and test [ADAM's Python bindings](#python), enable the `python`
profile:

```bash
mvn -Ppython package
```

This will enable the `adam-python` module as part of the ADAM build. This module
uses Maven to invoke a Makefile that builds a Python egg and runs tests. To
build this module, we require either an active [Conda](https://conda.io/) or
[virtualenv](https://virtualenv.pypa.io/en/stable/) environment.

[To setup and activate a Conda
environment](https://conda.io/docs/using/envs.html), run:

```bash
conda create -n adam python=2.7 anaconda
source activate adam
```

[To setup and activate a virtualenv
environment](https://virtualenv.pypa.io/en/stable/userguide/#usage), run:

```bash
virtualenv adam
. adam/bin/activate
```

Additionally, to run tests, the PySpark dependencies must be on the Python module
load path and the ADAM JARs must be built and provided to PySpark. This can be
done with the following bash commands:

```bash
# add pyspark to the python path
PY4J_ZIP="$(ls -1 "${SPARK_HOME}/python/lib" | grep py4j)"
export PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/${PY4J_ZIP}:${PYTHONPATH}

# put adam jar on the pyspark path
ASSEMBLY_DIR="${ADAM_HOME}/adam-assembly/target"
ASSEMBLY_JAR="$(ls -1 "$ASSEMBLY_DIR" | grep "^adam[0-9A-Za-z\.\_-]*\.jar$" | grep -v -e javadoc -e sources || true)"
export PYSPARK_SUBMIT_ARGS="--jars ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} --driver-class-path ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} pyspark-shell"
```

This assumes that the [ADAM JARs have already been built](#build-from-source).
Additionally, we require [pytest](https://docs.pytest.org/en/latest/) to be
installed. The adam-python makefile can install this dependency. Once you have
an active virtualenv or Conda environment, run:

```bash
cd adam-python
make prepare
```

## Building for R {#r-build}

ADAM supports SparkR, for Spark 2.1.0 and onwards. To build and test [ADAM's R
bindings](#r), enable the `r` profile:

```bash
mvn -Pr package
```

This will enable the `adam-r` module as part of the ADAM build. This module
uses Maven to invoke the `R` executable to build the `bdg.adam` package and run
tests. Beyond having `R` installed, we require you to have the `SparkR` package
installed, and the ADAM JARs must be built and provided to `SparkR`. This can be
done with the following bash commands:

```bash
# put adam jar on the SparkR path
ASSEMBLY_DIR="${ADAM_HOME}/adam-assembly/target"
ASSEMBLY_JAR="$(ls -1 "$ASSEMBLY_DIR" | grep "^adam[0-9A-Za-z\_\.-]*\.jar$" | grep -v javadoc | grep -v sources || true)"
export SPARKR_SUBMIT_ARGS="--jars ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} --driver-class-path ${ASSEMBLY_DIR}/${ASSEMBLY_JAR} sparkr-shell"
```

Note that the `ASSEMBLY_DIR` and `ASSEMBLY_JAR` lines are the same as for the
[Python build](#python-build). As with the Python build, this assumes that the
[ADAM JARs have already been built](#build-from-source).
