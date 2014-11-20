# Running ADAM on CDH 4 or 5

## Build Instructions

```bash
git clone https://github.com/bigdatagenomics/adam.git
cd adam
export "MAVEN_OPTS=-Xmx512m -XX:MaxPermSize=128m"
mvn package
```
The MAVEN_OPTS is necessary because the default heap & permgen space isn't enough to build the entire project.
JAR is available in adam/adam-cli/target/adam-0.6.1.jar

## Spark Settings

Spark Master environment variable must be set to connect to the master node, this defaults to local otherwise
TODO: Set this in CM instead of on each job, currently the default is the shortname which does not work

```bash
MASTER=spark://sparkmaster.yourdomain.com:7077 
```

This is necessary for Spark worker jobs to have classes in the JAR available to them.

```bash
SPARK_CLASSPATH=$ADAM_JAR
```

`spark.default.parallelism` controls the shuffle parallelism, Spark Doc suggestion is 2x cores, e.g.

```bash
SPARK_JAVA_OPTS+="-Dspark.default.parallelism=1024"
```

From Spark Docs: "consolidates intermediate files created during a shuffle ... It is recomended to set this to "true" when using ext4 or xfs filesystems"
TODO: Set this in CM instead of on each job

```bash
SPARK_JAVA_OPTS+=" -Dspark.shuffle.consolidateFiles=true" 
SPARK_JAVA_OPTS+=" -Dlog4j.configuration=log4j.properties"
```

## BAM/Read Processing

- The BAM processing commands are run through the ADAM  `transform` command.
- The final arguments to the `transform` commands are the input and output paths.  
- If the paths are on HDFS, they must be the full URI including the namenode address, i.e.  

```bash
INPUT=hdfs://namenode.yourdomain.com/path/to/bam
OUTPUT=hdfs://namenode.yourdomain.com/path/to/output
```

- All of the following commands, conversion, mark_duplicates and BQSR can be chained together and run in a single job.

You can monitor the job results at http://your-spark-master-node.com:9077

### Conversion

```bash
java -Xmx8g $SPARK_JAVA_OPTS -jar $ADAM_JAR transform \
-spark_master $MASTER \
-spark_jar $ADAM_JAR \
$INPUT \
$OUTPUT
```

### Mark Duplicates

This is operated by adding the `-mark_duplicate_reads` flag.

```bash
java -Xmx8g $SPARK_JAVA_OPTS -jar $ADAM_JAR transform \
-mark_duplicate_reads \
-spark_master $MASTER \
-spark_jar $ADAM_JAR \
$INPUT \
$OUTPUT.md

```
### BQSR

This is operated by adding the `-recalibrate_base_qualities` flag and specifying a dbSnp sites file.  The sites file is list of chromosome and position tab-separated pairs.

```bash
DBSNP=/path/to/dbsnp/dbsnp_137.b37.excluding_sites_after_129.sites

java -Xmx8g $SPARK_JAVA_OPTS -jar $ADAM_JAR transform \
-recalibrate_base_qualities \
-known_snps $DBSNP
-spark_master $MASTER \
-spark_jar $ADAM_JAR \
$INPUT \
$OUTPUT.bqsr
```
