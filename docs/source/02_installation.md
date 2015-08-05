# Building ADAM from Source

You will need to have [Maven](http://maven.apache.org/) installed in order to build ADAM.

> **Note:** The default configuration is for Hadoop 2.2.0. If building against a different
> version of Hadoop, please edit the build configuration in the `<properties>` section of
> the `pom.xml` file.

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

ADAM is packaged via [appassembler](http://mojo.codehaus.org/appassembler/appassembler-maven-plugin/) and includes all necessary
dependencies

You might want to add the following to your `.bashrc` to make running ADAM easier:

```bash
alias adam-submit="${ADAM_HOME}/bin/adam-submit"
alias adam-shell="${ADAM_HOME}/bin/adam-shell"
```

`$ADAM_HOME` should be the path to where you have checked ADAM out on your local filesystem. 
The first alias should be used for running ADAM jobs that operate locally. The latter two aliases 
call scripts that wrap the `spark-submit` and `spark-shell` commands to set up ADAM. You'll need
to have the Spark binaries on your system; prebuilt binaries can be downloaded from the
[Spark website](http://spark.apache.org/downloads.html). Currently, we build for
[Spark 1.1, and Hadoop 2.3 (CDH5)](http://d3kbcqa49mib13.cloudfront.net/spark-1.1.0-bin-hadoop2.3.tgz).

Once this alias is in place, you can run ADAM by simply typing `adam-submit` at the commandline, e.g.

```bash
$ adam-submit
```
Outputs:
```
     e            888~-_              e                 e    e
    d8b           888   \            d8b               d8b  d8b
   /Y88b          888    |          /Y88b             d888bdY88b
  /  Y88b         888    |         /  Y88b           / Y88Y Y888b
 /____Y88b        888   /         /____Y88b         /   YY   Y888b
/      Y88b       888_-~         /      Y88b       /          Y888b

Choose one of the following commands:

           transform : Convert SAM/BAM to ADAM format and optionally perform read pre-processing transformations
            flagstat : Print statistics on reads in an ADAM file (similar to samtools flagstat)
           reads2ref : Convert an ADAM read-oriented file to an ADAM reference-oriented file
             mpileup : Output the samtool mpileup text from ADAM reference-oriented data
               print : Print an ADAM formatted file
   aggregate_pileups : Aggregate pileups in an ADAM reference-oriented file
            listdict : Print the contents of an ADAM sequence dictionary
             compare : Compare two ADAM files based on read name
    compute_variants : Compute variant data from genotypes
            bam2adam : Single-node BAM to ADAM converter (Note: the 'transform' command can take SAM or BAM as input)
            adam2vcf : Convert an ADAM variant to the VCF ADAM format
            vcf2adam : Convert a VCF file to the corresponding ADAM format

```

ADAM outputs all the commands that are available for you to run. To get
help for a specific command, run `adam-submit <command>` without any additional arguments.

```bash
$ adam-submit transform --help
```
Outputs:
```
Argument "INPUT" is required
 INPUT                                  : The ADAM, BAM or SAM file to apply
                                          the transforms to
 OUTPUT                                 : Location to write the transformed
                                          data in ADAM/Parquet format
 -coalesce N                            : Set the number of partitions written
                                          to the ADAM output directory
 -dbsnp_sites VAL                       : dbsnp sites file
 -h (-help, --help, -?)                 : Print help
 -mark_duplicate_reads                  : Mark duplicate reads
 -parquet_block_size N                  : Parquet block size (default = 128mb)
 -parquet_compression_codec [UNCOMPRESS : Parquet compression codec
 ED | SNAPPY | GZIP | LZO]              :  
 -parquet_disable_dictionary            : Disable dictionary encoding
 -parquet_page_size N                   : Parquet page size (default = 1mb)
 -recalibrate_base_qualities            : Recalibrate the base quality scores
                                          (ILLUMINA only)
 -sort_reads                            : Sort the reads by referenceId and
                                          read position
 -spark_env KEY=VALUE                   : Add Spark environment variable
 -spark_home PATH                       : Spark home
 -spark_jar JAR                         : Add Spark jar
 -spark_master VAL                      : Spark Master (default = "local[#cores]
                                          ")

```
