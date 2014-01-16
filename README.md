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

In contrast, the [ADAM specification for storing reads](adam-format/src/main/resources/avro/adam.avdl) 
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
$ git clone git@github.com:bigdatagenomics/adam.git
$ cd adam
$ mvn clean package
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

            bam2adam : Converts a local BAM file to ADAM/Parquet and writes locally or to HDFS, S3, etc
           transform : Apply one of more transforms to an ADAM file and save the results to another ADAM file
            flagstat : Prints statistics for ADAM data similar to samtools flagstat
           reads2ref : Convert an ADAM read-oriented file to an ADAM reference-oriented file
             mpileup : Output the samtool mpileup text from ADAM reference-oriented data
               print : Print an ADAM formatted file
   aggregate_pileups : Aggregates pileups in an ADAM reference-oriented file

```

ADAM outputs all the commands that are available for you to run. To get
help for a specific command, run `adam <command> -h`, e.g.

````
$ adam bam2adam --help
 BAM                                    : The SAM or BAM file to convert
 ADAM                                   : Location to write ADAM data
 -h (-help, --help, -?)                 : Print help
 -num_threads N                         : Number of threads/partitions to use
                                          (default=4)
 -parquet_block_size N                  : Parquet block size (default = 128mb)
 -parquet_compression_codec [UNCOMPRESS : Parquet compression codec
 ED | SNAPPY | GZIP | LZO]              :  
 -parquet_disable_dictionary            : Disable dictionary encoding
 -parquet_page_size N                   : Parquet page size (default = 1mb)
 -queue_size N                          : Queue size (default = 10,000)
 -samtools_validation [STRICT |         : SAM tools validation level
 LENIENT | SILENT]                      :  
````

# bam2adam

Example
````
$ adam bam2adam NA12878_chr20.bam hdfs://user/genomics/NA12878_chr20.adam
````

The `bam2adam` command converts a BAM file into ADAM format. This command
will create `-num_threads` threads with each writing to a separate partition on the target filesystem.
For example, if you run with 4 threads and choose to write location `NA12878_chr20.adam`,
then the `bam2adam` command will create four files, e.g.

````
$ ls NA12878_chr20.adam/
part0   part1   part2   part3
````

Partitioning isn't required (you could set `-num_threads` to `1` for example); however, it
will improve your write performance significantly. Each thread does the SAMRecord to ADAMRecord
conversion using its own ParquetWriter to cache, organize and sync blocks of data. In general,
doubling the number of threads will cut the conversion time in half.

Keep in mind that each thread you request needed enough memory to run effectively. With the
default block size (128mb), you should budget for at least 512mb per thread. For example, if
you ran `bam2adam` with 16 threads, they will need about 8gb of memory. If you increase the
`-parquet_block_size`, then you will need proportionally more.

A good way to check that you're using good command-line options is to run `top` while you
run `bam2adam`. You should see that all threads are constantly running near 100% cpu. If
you see the CPU usage drop for long periods of time, it's very likely that you haven't 
provided ADAM with enough memory (check your `-Xmx` flag) and garbage collection is hurting
your performance.

Note that the `bam2adam` command does not currently guarantee that read ordering is maintained.

ADAM files are typically around 20% smaller than BAM files without any loss of information.
In practice, it takes an 'm2.4xlarge' ec2 node about 4 hours to convert a high-coverage, 
full-genome and write it to HDFS. Low coverage bams can
take just a few minutes. You can experiment with Snappy compression, using 
`-parquet_compression_codec snappy`, to reduce the time to convert to ADAM. However, the size
of the ADAM file will likely be 10% larger than BAM.

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

# Mailing List

[The ADAM mailing list](https://groups.google.com/forum/#!forum/adam-developers) is a good
way to sync up with other people who use ADAM including the core developers. You can subscribe
by sending an email to `adam-developers+subscribe@googlegroups.com` or just post using
the [web forum page](https://groups.google.com/forum/#!forum/adam-developers).

# License

ADAM is released under an [Apache 2.0 license](LICENSE.txt).
