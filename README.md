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
(https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/adam.avdl)
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
does help cost down on file seeks but the columnar store ADAM uses reduces seeks
even more.

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
Once successfully built, you'll see a single jar file named `adam-X.Y-SNAPSHOT.jar` in the `adam-cli/target` directory. This single jar
has all the dependencies you need in it. You can copy this single jar file to any machine you want to launch ADAM jobs from.

You might want to take a peek at the `scripts/jenkins-test` script and give it a run. It will fetch a mouse chromosome, encode it to ADAM
reads and pileups, run flagstat, etc. We use this script to test that ADAM is working correctly.

## Running ADAM

The ADAM jar file is a self-executing jar file with all dependencies included.

You might want to add the following to your `.bashrc` to make running `adam` easier:

```
adam_jar="/workspace/adam/adam-cli/target/adam-0.6.0-SNAPSHOT.jar"
alias adam="java -Xmx4g -jar $adam_jar"
```

Of course, you will want to change the `adam_jar` variable to point to the directory
you placed ADAM on your local filesystem. You can also modify `-Xmx4g` to either give
ADAM more or less memory depending on your system.

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
help for a specific command, run `adam <command> -h`, e.g.

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

# flagstat

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
time than samtools to compute these statistics. For example, on my MacBook Pro the command 
above took 17 seconds to run while `samtools flagstat NA12878_chr20.bam` took 55 secs.
On larger files, the difference in speed is even more dramatic. ADAM is faster because
it's multi-threaded and distributed and uses a columnar storage format (with a projected
schema that only materializes the read flags instead of the whole read). 

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
