# Introduction

ADAM is a genomics analysis platform with specialized file formats built using [Apache Avro](http://avro.apache.org), 
[Apache Spark](http://spark.incubator.apache.org/) and [Parquet](http://parquet.io/). Apache 2 licensed.

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

In contrast, the [ADAM specification for storing reads](https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl)
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
