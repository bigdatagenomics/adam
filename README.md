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
source code to encode and decode the records. This error prone and an unneccesary
hassle.

In contrast, the [ADAM specification for storing reads](src/main/resources/avro/adam.avdl) 
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
$ bin/hadoop jar adam-X.Y.jar [generic Hadoop options] module_name [module options]
```

## Working Example

Let's convert a FASTA and BAM file into the Avro/Parquet format using ADAM. For this example,
we convert two files, `reference.fasta` and `reads.bam`.

### Convert a FASTA file

The command..
```
$ bin/hadoop jar adam-X.Y.jar convert_fasta reference.fasta
```
...will launch a Hadoop Map-Reduce job to convert your fasta file. When the job
completes, you'll find all the ADAM data in a directory called
`reference.fasta.adam1`. You should also see a zero-sized file called
`_SUCCESS`.

NOTE: The hadoop command will use the `HADOOP_CONF_DIR` environment variable to find your
Hadoop configuration files. Alternatively, you can just use the `-config` generic option, e.g.
`hadoop jar -config /path/to/hadoop-site.xml convert_fasta ...`. 

### Convert a BAM file

It's just as easy to convert a BAM file.
```
$ bin/hadoop jar adam-X.Y.jar convert_bam reads.bam
```
This command also runs a Hadoop Map-Reduce job to convert the data. When the job completes,
you'll find all the ADAM data in the directory `reads.bam.adam1` along with a file called `_SUCCESS`.

The ADAM equivalent of a BAM file is currently about ~15% smaller for the same data. Columns data is 
stored in contiguous chunks and compressed together. Since many columns contain redundant information, 
e.g. reference name, mapq, the data compresses very well. As Parquet adds more feature like run-length 
encoding and dictionary encoding (coming soon), you can expect the size of files to drop even more.

NOTE: The default replication factor for Hadoop is 3 which means that each block you write is stored
in three locations. This replication protects the data from machine/disk failures. For performance,
you can use `hadoop jar adam-X.Y.jar -D dfs.replication=1 convert_bam...`.

### Create an ADAM pileup file

ADAM also allows you to join data from an ADAM reference and read file into a
pileup datastore in a single Map-Reduce job. 

The reads are filtered identically to the [GATK loci walker](http://www.broadinstitute.org/gatk/guide/article?id=1351). Reads that are unmapped, non-primary aligned,
duplicates or fail vendor quality checks are dropped. You can also use the
`-mapq` option to set the threshold for filtering reads with a low mapq value.
By default, any reads with a mapq less than 30 are dropped.

To create a pileup file, run the following command e.g.

```
$ bin/hadoop jar adam-X.Y.jar pileup -reference reference.fasta.adam1 -reads reads.bam.adam1 mypileup
```
When this command completes, you have a new pileup datastore called `mypileup`.

In some cases, the reference names in your reads file don't perfectly match the reference names. In that case,
you might see null values for the `referenceName` and `referenceBase`. To workaround this problem
use the -rename_reference option, e.g. `-rename_reference 11=chr11 -rename_reference 12=chr12`. In 
this example, any read with the reference `11` will be joined to `chr11` reference data and any read with
reference `12` will be joined to the `chr12` reference.

The `pileup` command uses the Hadoop `MultiInputs` input format in order to join the reference and read
data in a single Map-Reduce job. For performance, the reference and read data are split
into "buckets" at specific positions along the genome. The number of positions in each bucket is controlled
by the `-step` option which defaults to 1000. This means that the genome is split into buckets that are
are 1000 positions wide and contain all the read and reference data necessary for the reduce step. Increasing 
this number will improve performance at the cost of memory. If you have limited memory, reducing the step 
can reduce memory use.

You can use the `print` command (explained below) to see the contents of the pileup file.

```
$ bin/hadoop jar adam-X.Y.jar print mypileup
...
{"referenceName": "chr11", "position": 782372, "referenceBase": "G", "pileup": "GGGGGGGGGGG", "qualities": "8EAIK*EAI2%"}
{"referenceName": "chr11", "position": 782373, "referenceBase": "T", "pileup": "TGTTTTTTTTC", "qualities": "\/J?F<14II=$"}
{"referenceName": "chr11", "position": 782374, "referenceBase": "C", "pileup": "CCCCCCCCCCC", "qualities": "A@KAH5JIKI*"}
{"referenceName": "chr11", "position": 782375, "referenceBase": "C", "pileup": "CGCCCCCCCCT", "qualities": "CK:KK.KKI-5"}
{"referenceName": "chr11", "position": 782376, "referenceBase": "C", "pileup": "CGCCCCCCCCC", "qualities": "HKHMJIMHKJ%"}
{"referenceName": "chr11", "position": 782377, "referenceBase": "A", "pileup": "AGAAAAAAAAC", "qualities": "AG?GGCIDJF."}
{"referenceName": "chr11", "position": 782378, "referenceBase": "C", "pileup": "CTCCCCCCCCC", "qualities": "FKKHI@HFMK\/"}
{"referenceName": "chr11", "position": 782379, "referenceBase": "A", "pileup": "ACAAAAAAAAT", "qualities": "EH4GI,=IAD8"}
```
The pileup field is a string with one character for each read. The characters represent
the base aligned at that position for each read. The qualities string represents the
phred quality scores for each of the bases in the pileup string.

### Print the contents of ADAM data

ADAM has a simple module for printing the contents of your data to console. For example, to view your fasta file, run the following:
```
$ bin/hadoop jar adam-X.Y.jar print reference.fasta.adam1
```

Of course, to really use this data, you'll want to use tools like Shark/Spark or Impala.

### Shark or Impala Example

TODO: The Parquet Hive SerDe is nearly polished and ready to ship. When it is, I'll drop an example here.

### Pig Example

Parquet has support for Pig.

You can find an [example of a Pig script](examples/good_reads.pig) in the `examples` directory of ADAM. This pig scripts generates the total number of reads per reference with the
total number reads that were mapped with good quality. This isn't meant to be a
general-purpose or even a well-written script (as it's the first pig script
I've ever written).

```
$ pig -f examples/good_reads.pig
```
This scripts will output its results to a directory called `results` in your home directory. Here's the output for the ADAM1 file I processed.
```
chr1	95301810	chr1	81219513
chr2	150651983	chr2	86589043
chr3	76637966	chr3	65233010
chr4	77756231	chr4	64257935
chr5	76236808	chr5	63658192
chr6	75626876	chr6	63004588
chr7	72840307	chr7	58128351
chr8	63917066	chr8	53227910
...
chrX_GL456233_random	135674	chrX_GL456233_random	117667
```
The second column shows the total number of reads. The fourth shows the total
number of those reads that were mapped with good quality.

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
