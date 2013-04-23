ADAM
====

*ADAM: Datastore Alignment Map*

# Introduction

Current genomic file formats are loosely defined and not designed for
distributed processing. ADAM addresses these problems by explicitly defining data
formats as [Apache Avro](http://avro.apache.org) objects and storing them in 
[Parquet](http://parquet.io) files.

## Explicitly defined format

For example, the [Sequencing Alignment Map (SAM) and Binary Alignment Map (BAM) 
file specification](http://samtools.sourceforge.net/SAM1.pdf) defines a data format 
for storing reads from aligners. The specification is well-written but provides
no tools for developers to implement the format. Developers have to hand-craft 
source code to encode and decode the records. 

In contrast, the [ADAM specification for storing reads](src/main/resources/avro/adam.avdl) 
is defined in the Avro Interface Description Language (IDL) which is directly converted
into source code. Avro supports a number of computer languages. ADAM uses Java; you could 
just as easily use this Avro IDL description as the basis for a Python project.

## Ready for distributed processing

The SAM/BAM format is record-oriented with a single record for each read. However,
the typical data access pattern is column oriented, e.g. search for bases read at
specific position in a reference genome. The BAM specification tries to support
this pattern by defining a format for a separate index file. However, this index
needs to be regenerated anytime your BAM file changes which is costly. The index
does help cost down on file seeks but the columnar store ADAM uses reduces seek
costs even more.

ADAM stores data in a column-oriented format, [Parquet](http://parquet.io), which
improves search performance and compression without an index. In addition, Parquet
data is designed to be splittable and work well with distributed systems like
Hadoop. ADAM supports Hadoop 1.x and Hadoop 2.x systems. 

Once you convert your BAM file, it can be directly accessed by 
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
Once successfully built, you'll see a single jar file named `adam-X.Y-SNAPSHOT.jar` in the root directory. This jar 
can be used with any version of Hadoop and has the the dependencies you need in it. You could, for example, copy 
this single jar file to other machines you want to launch ADAM jobs from.

## Running ADAM

To see all the available ADAM modules, run the following command:

```
$ bin/hadoop jar adam-X.Y.jar
```

You will receive a listing of all modules and how to launch them. The commandline syntax to
run a module is:

```
$ bin/hadoop jar adam-X.Y.jar [generic Hadoop options] moduleName [module options]
```

## Working Example

Let's convert a FASTA and BAM file into the Avro/Parquet format using ADAM. For this example,
we convert two files, `reference.fasta` and `reads.bam`.

The command..
```
$ bin/hadoop jar adam-X.Y.jar import_fasta -fasta reference.fasta
```
...will launch a Hadoop Map-Reduce job to convert your fasta file. When the job
completes, you'll find all the ADAM data in a directory called
`reference.fasta.adam1`. You should also see a zero-sized file called
`_SUCCESS`.

NOTE: The hadoop command will use the `HADOOP_CONF_DIR` environment variable to find your
Hadoop configuration files. Alternatively, you can just use the `-config` generic option, e.g.
`hadoop jar -config /path/to/hadoop-site.xml import_fasta ...`. 

It's just as easy to import a BAM file.
```
$ bin/hadoop jar adam-X.Y.jar import_bam -bam reads.bam
```
This command also runs a Hadoop Map-Reduce job to convert the data. When the job completes,
you'll find all the ADAM data in the directory `reads.bam.adam1` along with a file called `_SUCCESS`.

The ADAM equivalent of a BAM file is currently about ~15% smaller for the same data. Columns data is 
stored in contiguous chunks and compressed together. Since many columns contain redundant information, 
e.g. reference name, mapq, the data compresses very well. As Parquet adds more feature like run-length 
encoding and dictionary encoding (coming soon), you can expect the size of files to drop even more.

NOTE: The default replication factor for Hadoop is 3 which means that each block you write is stored
in three locations. This replication protects the data from machine/disk failures. For performance,
you can use `hadoop jar adam-X.Y.jar -D dfs.replication=1 import_bam...`.

ADAM has a simple module for printing the contents of your data. For example, to view your fasta
file as CSV, run the following:
```
$ bin/hadoop jar adam-X.Y.jar print -input reference.fasta -output reference.csv
```
The `print` module has other options as well, e.g.
```
 -column_delimiter DELIMITER : The record delimiter. Default is ','
 -exclude COLUMN_NAME        : Columns to exclude in output. Default is none.
 -include COLUMN_NAME        : Columns to include in output. Default is all.
```
which allow you control which columns are materialized and what delimiter is used. Since ADAM
store the data column-oriented, only the column you request are read from HDFS.

Of course, to really use this data, you'll want to use tools like Shark/Spark or Impala.

TODO: Provide examples

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
