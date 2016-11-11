# Building ADAM from Source

You will need to have [Maven](http://maven.apache.org/) installed in order to build ADAM.

> **Note:** The default configuration is for Hadoop 2.6.0. If building against a different
> version of Hadoop, please pass `-Dhadoop.version=<HADOOP_VERSION>` to the Maven command.
> ADAM will cross-build for both Spark 1.x and 2.x, but builds by default against Spark
> 1.6.3. To build for Spark 2, run the `./scripts/move_to_spark2.sh` script.

```bash
$ git clone https://github.com/bigdatagenomics/adam.git
$ cd adam
$ export "MAVEN_OPTS=-Xmx512m -XX:MaxPermSize=128m"
$ mvn clean package -DskipTests
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
call scripts that wrap the `spark-submit` and `spark-shell` commands to set up ADAM. You'll need
to have the Spark binaries on your system; prebuilt binaries can be downloaded from the
[Spark website](http://spark.apache.org/downloads.html). Our [continuous integration setup](
https://amplab.cs.berkeley.edu/jenkins/job/ADAM/) builds ADAM against Spark 1.4.1, 1.5.2, 1.6.2,
and 2.0.0, as well as Hadoop 2.3.0 and 2.6.0.

Once this alias is in place, you can run ADAM by simply typing `adam-submit` at the commandline, e.g.

```bash
$ adam-submit
```
