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
chr9	102178816	chr9	54825123
chrM	163064	chrM	122442
chrX	78315237	chrX	58536629
chrY	1466505	chrY	131383
chr10	63859164	chr10	54853408
chr11	62488811	chr11	54323384
chr12	62148753	chr12	48265330
chr13	60153448	chr13	50737381
chr14	62131272	chr14	47229276
chr15	50375498	chr15	43728272
chr16	47045132	chr16	40740844
chr17	47566119	chr17	39706961
chr18	44173724	chr18	38528221
chr19	29935623	chr19	25853204
chrUn_GL456239	72403	chrUn_GL456239	57619
chrUn_GL456359	12874	chrUn_GL456359	9841
chrUn_GL456360	15496	chrUn_GL456360	12709
chrUn_GL456366	22058	chrUn_GL456366	15176
chrUn_GL456367	27210	chrUn_GL456367	14398
chrUn_GL456368	14015	chrUn_GL456368	6969
chrUn_GL456370	43814	chrUn_GL456370	9682
chrUn_GL456372	22615	chrUn_GL456372	6403
chrUn_GL456378	24985	chrUn_GL456378	15049
chrUn_GL456379	29944	chrUn_GL456379	24921
chrUn_GL456381	11458	chrUn_GL456381	9352
chrUn_GL456382	8492	chrUn_GL456382	5740
chrUn_GL456383	37685	chrUn_GL456383	12333
chrUn_GL456385	15814	chrUn_GL456385	10964
chrUn_GL456387	21579	chrUn_GL456387	11640
chrUn_GL456389	120899	chrUn_GL456389	11857
chrUn_GL456390	18044	chrUn_GL456390	5366
chrUn_GL456392	354634	chrUn_GL456392	20895
chrUn_GL456393	36186	chrUn_GL456393	22781
chrUn_GL456394	14680	chrUn_GL456394	11022
chrUn_GL456396	39996	chrUn_GL456396	8321
chrUn_JH584304	16855781	chrUn_JH584304	482761
chr1_GL456210_random	183811	chr1_GL456210_random	25415
chr1_GL456211_random	286558	chr1_GL456211_random	42625
chr1_GL456212_random	238661	chr1_GL456212_random	26401
chr1_GL456221_random	257674	chr1_GL456221_random	23334
chr4_GL456216_random	28493	chr4_GL456216_random	23587
chr4_GL456350_random	69348	chr4_GL456350_random	28
chr4_JH584292_random	10539	chr4_JH584292_random	6399
chr4_JH584293_random	64221	chr4_JH584293_random	103
chr4_JH584295_random	442	chr4_JH584295_random	341
chr5_GL456354_random	97776	chr5_GL456354_random	2071
chr5_JH584296_random	85705	chr5_JH584296_random	795
chr5_JH584297_random	92028	chr5_JH584297_random	953
chr5_JH584298_random	89485	chr5_JH584298_random	24
chr5_JH584299_random	502767	chr5_JH584299_random	27836
chr7_GL456219_random	46743	chr7_GL456219_random	27
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
