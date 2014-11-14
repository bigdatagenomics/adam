# Example of Running ADAM

ADAM is packaged via [appassembler](http://mojo.codehaus.org/appassembler/appassembler-maven-plugin/) and includes all necessary
dependencies

You might want to add the following to your `.bashrc` to make running `adam` easier:

```
alias adam-local="bash ${ADAM_HOME}/adam-cli/target/appassembler/bin/adam"
alias adam-submit="${ADAM_HOME}/bin/adam-submit"
alias adam-shell="${ADAM_HOME}/bin/adam-shell"
```

`$ADAM_HOME` should be the path to where you have checked ADAM out on your local filesystem. 
The first alias should be used for running ADAM jobs that operate locally. The latter two aliases 
call scripts that wrap the `spark-submit` and `spark-shell` commands to set up ADAM. You'll need
to have the Spark binaries on your system; prebuilt binaries can be downloaded from the
[Spark website](http://spark.apache.org/downloads.html). Currently, we build for
[Spark 1.1, and Hadoop 2.3 (CDH5)](http://d3kbcqa49mib13.cloudfront.net/spark-1.1.0-bin-hadoop2.3.tgz).

Once this alias is in place, you can run adam by simply typing `adam` at the commandline, e.g.

```
$ adam

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
help for a specific command, run `adam <command>` without any additional arguments.

````
$ adam transform --help
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

````

## flagstat

Once you have data converted to ADAM, you can gather statistics from the ADAM file using `flagstat`.
This command will output stats identically to the samtools `flagstat` command, e.g.

````
$ adam flagstat NA12878_chr20.adam
51554029 + 0 in total (QC-passed reads + QC-failed reads)
0 + 0 duplicates
50849935 + 0 mapped (98.63%:0.00%)
51554029 + 0 paired in sequencing
25778679 + 0 read1
25775350 + 0 read2
49874394 + 0 properly paired (96.74%:0.00%)
50145841 + 0 with itself and mate mapped
704094 + 0 singletons (1.37%:0.00%)
158721 + 0 with mate mapped to a different chr
105812 + 0 with mate mapped to a different chr (mapQ>=5)
````

In practice, you'll find that the ADAM `flagstat` command takes orders of magnitude less
time than samtools to compute these statistics. For example, on a MacBook Pro the command 
above took 17 seconds to run while `samtools flagstat NA12878_chr20.bam` took 55 secs.
On larger files, the difference in speed is even more dramatic. ADAM is faster because
it's multi-threaded and distributed and uses a columnar storage format (with a projected
schema that only materializes the read flags instead of the whole read). 

## Running on a cluster

We provide the `adam-submit` and `adam-shell` commands under the `bin` directory. These can
be used to submit ADAM jobs to a spark cluster, or to run ADAM interactively.

## Running Plugins

ADAM allows users to create plugins via the [ADAMPlugin](https://github.com/bigdatagenomics/adam/blob/master/adam-core/src/main/scala/org/bdgenomics/adam/plugins/ADAMPlugin.scala)
trait. These plugins are then imported using the Java classpath at runtime. To add to the classpath when
using appassembler, use the `$CLASSPATH_PREFIX` environment variable. For an example of how to use
the plugin interface, please see the [adam-plugins repo](https://github.com/heuermh/adam-plugins).
