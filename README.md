ADAM
====
A genomics processing engine and specialized file format built using [Apache Avro](http://avro.apache.org), 
[Apache Spark](http://spark.incubator.apache.org/) and [Parquet](http://parquet.io/). Apache 2 licensed.

# Introduction

Current genomic file formats are not designed for
distributed processing. ADAM addresses this by explicitly defining data
formats as [Apache Avro](http://avro.apache.org) objects and storing them in 
[Parquet](http://parquet.io) files. [Apache Spark](http://spark.incubator.apache.org/)
is used as the cluster execution system.

## Explicitly defined format

The [Sequencing Alignment Map (SAM) and Binary Alignment Map (BAM)
file specification](http://samtools.sourceforge.net/SAM1.pdf) defines a data format 
for storing reads from aligners. The specification is well-written but provides
no tools for developers to implement the format. Developers have to hand-craft 
source code to encode and decode the records which is error prone and an unneccesary
hassle.

In contrast, the [ADAM specification for storing reads]
(https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl)
is defined in the Avro Interface Description Language (IDL) which is directly converted
into source code. Avro supports a number of computer languages. ADAM uses Java; you could 
just as easily use this Avro IDL description as the basis for a Python project. Avro
currently supports c, c++, csharp, java, javascript, php, python and ruby. 

## Ready for distributed processing

The SAM/BAM format is record-oriented with a single record for each read. However,
the typical data access pattern is column oriented, e.g. search for bases at a
specific position in a reference genome. The BAM specification tries to support
this pattern by defining a format for a separate index file. However, this index
needs to be regenerated anytime your BAM file changes which is costly. The index
does help keep the cost down on file seeks but the columnar store ADAM uses reduces
the cost of seeks even more.

Once you convert your BAM file to ADAM, it can be directly accessed by 
[Hadoop Map-Reduce](http://hadoop.apache.org), [Spark](http://spark-project.org/), 
[Shark](http://shark.cs.berkeley.edu), [Impala](https://github.com/cloudera/impala), 
[Pig](http://pig.apache.org), [Hive](http://hive.apache.org), whatever. Using
ADAM will unlock your genomic data and make it available to a broader range of
systems.

# Getting Started

## Installation

You will need to have [Maven](http://maven.apache.org/) installed in order to build ADAM.

> **Note:** The default configuration is for Hadoop 2.2.0. If building against a different
> version of Hadoop, please edit the build configuration in the `<properties>` section of
> the `pom.xml` file.

```
$ git clone https://github.com/bigdatagenomics/adam.git
$ cd adam
$ export "MAVEN_OPTS=-Xmx512m -XX:MaxPermSize=128m"
$ mvn clean package -DskipTests
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

Once this alias is in place, you can run adam by simply typing `adam-local` at the commandline, e.g.

```
$ adam-local

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
help for a specific command, run `adam-local <command>` without any additional arguments.

````
$ adam-submit transform
Argument "INPUT" is required
 INPUT                                                           : The ADAM, BAM or SAM file to apply the transforms to
 OUTPUT                                                          : Location to write the transformed data in ADAM/Parquet format
 -coalesce N                                                     : Set the number of partitions written to the ADAM output directory
 -dump_observations VAL                                          : Local path to dump BQSR observations to. Outputs CSV format.
 -h (-help, --help, -?)                                          : Print help
 -known_indels VAL                                               : VCF file including locations of known INDELs. If none is provided, default
                                                                   consensus model will be used.
 -known_snps VAL                                                 : Sites-only VCF giving location of known SNPs
 -log_odds_threshold N                                           : The log-odds threshold for accepting a realignment. Default value is 5.0.
 -mark_duplicate_reads                                           : Mark duplicate reads
 -max_consensus_number N                                         : The maximum number of consensus to try realigning a target region to. Default
                                                                   value is 30.
 -max_indel_size N                                               : The maximum length of an INDEL to realign to. Default value is 500.
 -max_target_size N                                              : The maximum length of a target region to attempt realigning. Default length is
                                                                   3000.
 -parquet_block_size N                                           : Parquet block size (default = 128mb)
 -parquet_compression_codec [UNCOMPRESSED | SNAPPY | GZIP | LZO] : Parquet compression codec
 -parquet_disable_dictionary                                     : Disable dictionary encoding
 -parquet_logging_level VAL                                      : Parquet logging level (default = severe)
 -parquet_page_size N                                            : Parquet page size (default = 1mb)
 -print_metrics                                                  : Print metrics to the log on completion
 -qualityBasedTrim                                               : Trims reads based on quality scores of prefix/suffixes across read group.
 -qualityThreshold N                                             : Phred scaled quality threshold used for trimming. If omitted, Phred 20 is used.
 -realign_indels                                                 : Locally realign indels present in reads.
 -recalibrate_base_qualities                                     : Recalibrate the base quality scores (ILLUMINA only)
 -repartition N                                                  : Set the number of partitions to map data to
 -sort_fastq_output                                              : Sets whether to sort the FASTQ output, if saving as FASTQ. False by default.
                                                                   Ignored if not saving as FASTQ.
 -sort_reads                                                     : Sort the reads by referenceId and read position
 -trimBeforeBQSR                                                 : Performs quality based trim before running BQSR. Default is to run quality based
                                                                   trim after BQSR.
 -trimFromEnd N                                                  : Trim to be applied to end of read.
 -trimFromStart N                                                : Trim to be applied to start of read.
 -trimReadGroup VAL                                              : Read group to be trimmed. If omitted, all reads are trimmed.
 -trimReads                                                      : Apply a fixed trim to the prefix and suffix of all reads/reads in a specific read
                                                                   group.

````

If you followed along above, now try making your first `.adam` file like this:

````
adam-submit transform $ADAM_HOME/adam-core/src/test/resources/small.sam /tmp/small.adam
````

# flagstat

Once you have data converted to ADAM, you can gather statistics from the ADAM file using `flagstat`.
This command will output stats identically to the samtools `flagstat` command.

If you followed along above, now try gathering some statistics:

````
$ adam-local flagstat /tmp/small.adam
20 + 0 in total (QC-passed reads + QC-failed reads)
0 + 0 primary duplicates
0 + 0 primary duplicates - both read and mate mapped
0 + 0 primary duplicates - only read mapped
0 + 0 primary duplicates - cross chromosome
0 + 0 secondary duplicates
0 + 0 secondary duplicates - both read and mate mapped
0 + 0 secondary duplicates - only read mapped
0 + 0 secondary duplicates - cross chromosome
20 + 0 mapped (100.00%:0.00%)
0 + 0 paired in sequencing
0 + 0 read1
0 + 0 read2
0 + 0 properly paired (0.00%:0.00%)
0 + 0 with itself and mate mapped
0 + 0 singletons (0.00%:0.00%)
0 + 0 with mate mapped to a different chr
0 + 0 with mate mapped to a different chr (mapQ>=5)
````

In practice, you'll find that the ADAM `flagstat` command takes orders of magnitude less
time than samtools to compute these statistics. For example, on a MacBook Pro
`flagstat NA12878_chr20.bam` took 17 seconds to run while `samtools flagstat NA12878_chr20.bam`
took 55 seconds. On larger files, the difference in speed is even more dramatic. ADAM is faster
because it's multi-threaded and distributed and uses a columnar storage format (with a
projected schema that only materializes the read flags instead of the whole read). 

# Running on a cluster

We provide the `adam-submit` and `adam-shell` commands under the `bin` directory. These can
be used to submit ADAM jobs to a spark cluster, or to run ADAM interactively.

## Running Plugins

ADAM allows users to create plugins via the [ADAMPlugin](https://github.com/bigdatagenomics/adam/blob/master/adam-core/src/main/scala/org/bdgenomics/adam/plugins/ADAMPlugin.scala)
trait. These plugins are then imported using the Java classpath at runtime. To add to the classpath when
using appassembler, use the `$CLASSPATH_PREFIX` environment variable. For an example of how to use
the plugin interface, please see the [adam-plugins repo](https://github.com/heuermh/adam-plugins).

# Getting In Touch

## Mailing List

[The ADAM mailing list](https://groups.google.com/forum/#!forum/adam-developers) is a good
way to sync up with other people who use ADAM including the core developers. You can subscribe
by sending an email to `adam-developers+subscribe@googlegroups.com` or just post using
the [web forum page](https://groups.google.com/forum/#!forum/adam-developers).

## IRC Channel

A lot of the developers are hanging on the [#adamdev](http://webchat.freenode.net/?channels=adamdev)
freenode.net channel. Come join us and ask questions.

# License

ADAM is released under an [Apache 2.0 license](LICENSE.txt).
