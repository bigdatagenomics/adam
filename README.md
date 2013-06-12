ADAM
====

*ADAM: Datastore Alignment Map*

# Introduction

Current genomic file formats are loosely defined and not designed for
distributed processing. ADAM addresses these problems by explicitly defining data
formats as [Apache Avro](http://avro.apache.org) objects and storing them in 
[Parquet](http://parquet.io) files using [Apache Spark](http://spark.incubator.apache.org/).

## Explicitly defined format

For example, the [Sequencing Alignment Map (SAM) and Binary Alignment Map (BAM) 
file specification](http://samtools.sourceforge.net/SAM1.pdf) defines a data format 
for storing reads from aligners. The specification is well-written but provides
no tools for developers to implement the format. Developers have to hand-craft 
source code to encode and decode the records. This error prone and an unneccesary
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
does help cost down on file seeks but the columnar store ADAM uses reduces seek
costs even more.

ADAM stores data in a column-oriented format, [Parquet](http://parquet.io), which
improves search performance and compression without an index. In addition, Parquet
data is designed to be splittable and work well with distributed systems like
Hadoop. ADAM supports Hadoop 1.x and Hadoop 2.x systems out of the box.

Once you convert your BAM file to ADAM, it can be directly accessed by 
[Hadoop Map-Reduce](http://hadoop.apache.org), [Spark](http://spark-project.org/), 
[Shark](http://shark.cs.berkeley.edu), [Impala](https://github.com/cloudera/impala), 
[Pig](http://pig.apache.org), [Hive](http://hive.apache.org), whatever. Using
ADAM will unlock your genomic data and make it available to a broader range of
systems.

# Getting Started

## Installation

You will need to have [Maven](http://maven.apache.org/) installed in order to build ADAM.
```
$ git clone git@github.com:massie/adam.git
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
Once successfully built, you'll see a single jar file named `adam-X.Y-SNAPSHOT.jar` in the `adam-commands/target` directory. This jar 
has the the dependencies you need in it. You could, for example, copy this single jar file to other machines you want to launch ADAM jobs from.

## Running ADAM

The ADAM jar file is a self-executing jar file with all dependencies included.

To run ADAM, use the following command:

```
$ java -jar adam-X.Y-SNAPSHOT.jar

     e            888~-_              e                 e    e
    d8b           888   \            d8b               d8b  d8b
   /Y88b          888    |          /Y88b             d888bdY88b
  /  Y88b         888    |         /  Y88b           / Y88Y Y888b
 /____Y88b        888   /         /____Y88b         /   YY   Y888b
/      Y88b       888_-~         /      Y88b       /          Y888b

Choose one of the following commands:

            bam2adam : Convert a SAM/BAM file to ADAM read-oriented format
           reads2ref : Convert an ADAM read-oriented file to an ADAM reference-oriented file
             mpileup : Output the samtool mpileup text from ADAM reference-oriented data
               print : Print an ADAM formatted file
```

You could also add this command as an `alias` to your `.bashrc` e.g.,

```
alias adam="java -Xmx2g -jar /workspace/adam/adam-commands/target/adam-X.Y-SNAPSHOT.jar"
```

As you can see, ADAM outputs all the commands that are available for you to run. To get
help for a specific command, run `adam <command> -h`, e.g.

```
$ adam bam2adam -h
 BAM                            : The SAM or BAM file to convert
 ADAM                           : Location to write ADAM data
 -h (-help, --help, -?)         : print help
 -parquet_block_size N          : Parquet block size (default=512mb)
 -parquet_compress              : Parquet compress (default = true)
 -parquet_compression_codec VAL : Parquet compression codec (default=gzip)
 -parquet_disable_dictionary    : Disable dictionary encoding. (default = false)
 -parquet_page_size N           : Parquet page size (default=1mb)
 -spark_master VAL              : Spark Master (default=local)
````

# License

ADAM is released under an [Apache 2.0 license](LICENSE.txt).

# Implementation Notes

### Hadoop-BAM

ADAM wouldn't be possible without [Hadoop-BAM](http://sourceforge.net/projects/hadoop-bam/). For now, Hadoop-BAM
source is included as source in order work around some issues: a broken FASTA FileInput and Hadoop 1/2 API
imcompatibilities. As luck would have it, one of the Hadoop-BAM authors works with me in the 
[AMPLab](http://amplab.cs.berkeley.edu/). I'll work with him to submit the code back to Hadoop-BAM and change
ADAM to depend on binary artifacts soon.

### Parquet

TODO: The SAM string header is not currently saved to the Parquet file metadata section.

# Future Work

I'm planning on integrating ADAM with GATK. In particular, it should be straight-forward to create
adapters for the base walkers (e.g. LocusWalker, ReadWalker) in GATK.

# Support

Feel free to contact me directly if you have any questions about ADAM. My email address is `massie@cs.berkeley.edu`.
