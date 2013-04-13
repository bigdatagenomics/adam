ADAM
====

*[Avro](http://avro.apache.org/) Datafile for Alignment/Mapping (ADAM)*

# Introduction

ADAM is a file format as well as a light-weight framework for doing Genome Analysis.

## The ADAM framework

ADAM is written to be modular. To create a new module, simply extend the 
[AdamModule](src/main/java/edu/berkeley/amplab/adam/modules/AdamModule.java) abstract
class and define your module options using [args4j](http://args4j.kohsuke.org/). The
[CountReads](src/main/java/edu/berkeley/amplab/adam/modules/CountReads.java) class
is a good simple example to look at. Add your module to the `AdamMain` class and it
will appear in the module list. In the future, ADAM will support dynamically loaded
modules.

## ADAM File Format

The ADAM file format is an improvement on the SAM or BAM file formats in a number of ways:

1. The ADAM file format is easily splittable for distributed processing with Hadoop
2. The ADAM file format is completely self-contained. Each read includes the reference
information.
3. The ADAM file format is [defined in the Avro IDL](src/main/resources/avro/protocol.avdl) 
that makes it easy to create implementations in many different computer languages. This schema
is stored in the header of each ADAM file to ensure the data is self-descriptive.
4. The ADAM file format is compact. It holds more information about each read (e.g. reference
name, reference length) while still being about the same size as a BAM file. You can, of course, increase
the compression level to make an ADAM file smaller than a BAM file at the cost of encoding time.
5. The ADAM file has all the information needed to encode the data later as a SAM/BAM file if needed.
The entire SAM header is stored in the Avro meta-data with key `sam.header`.
6. The ADAM file format can be viewed in human-readable form as JSON using Avro tools

# Getting Started

## Installation

You will need to have [Maven](http://maven.apache.org/) installed in order to build this project. 
You will need to have [Hadoop](http://hadoop.apache.org/) or 
[CDH](http://www.cloudera.com/content/cloudera/en/products/cdh.html) installed in order to run it.

```
$ git clone git@github.com:massie/adam.git
$ cd adam
$ mvn package
```

Maven will create a self-executing jar, e.g. adam-X.Y.jar, in the project root that is ready to be 
used with Hadoop.

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

For example, let's say we wanted to convert a SAM/BAM file to an ADAM file and upload it on-the-fly, you
would use a commandline similar to the following:

```
$ bin/hadoop jar /workspace/adam/adam-0.1-SNAPSHOT.jar \
-conf ~/.whirr/testcluster/hadoop-site.xml \
convert -input NA12878_chr20.bam -output /user/matt/NA12878_chr20.avro
```

This will convert and `NA12878_chr20.bam` file and send it to `/user/matt/NA12878_chr20.avro` directly.
To see all the options for the `convert` module, run the it without any options, e.g.

```
$ bin/hadoop jar adam-X.Y.jar convert
```

## A step-by-step example

This example will show you how to convert a BAM file to an ADAM file and then count the number of reads.

First, we need to convert the BAM file to an ADAM file and upload it our Hadoop cluster.

```
$ bin/hadoop jar /workspace/adam/adam-0.1-SNAPSHOT.jar \
  -conf ~/.whirr/testcluster/hadoop-site.xml \
  convert -input NA12878_chr20.bam -output /user/matt/NA12878_chr20.avro
```
Note that you can also use the `HADOOP_CONF_DIR` variable if you like instead of the `-config` generic option.

ADAM will provide feedback about the reference being converted as well as the locus. When it finishes,
you should see a message similar to `X secs to convert Y reads`.

Now that your ADAM file stored in Hadoop, you can run analysis on it. Let's count the number
of reads per reference in the ADAM file using the `count_reads` module.

```
$ bin/hadoop jar /workspace/adam/adam-0.1-SNAPSHOT.jar  \
-conf ~/.whirr/testcluster/hadoop-site.xml \
count_reads -input /user/matt/NA12878_chr20.avro -output /user/matt/results
```

The `results` directory will contain the output of the reducer, e.g.

```
$ bin/hadoop fs -ls /user/matt/results
/user/matt/results/_SUCCESS
/user/matt/results/part-00000.avro
```

Let's look at the content of the results.

```
$ bin/hadoop fs -get /user/matt/results/part-00000.avro .
$ avrotools tojson /tmp/results/part-00000.avro 
{"key":"chr20","value":51554029}
```

This ADAM file had 51554029 reads on a single reference `chr20` (chromosome 20). Note that `avrotools` is
included with the [Apache Avro](http://avro.apache.org/) distribution.

The results are stored as an Avro file to make it easy to use as input to another job.

# License

ADAM is released under an [Apache 2.0 license](LICENSE.txt).

# Future Work

If you're interested in helping with this project, here are things to do. Feel free to fork away and send
me a pull request.

* Add ability to run GATK walkers inside modules (I have a good idea how to do this. Protyping now.).
* Write tests 
* Support dynamically loaded modules
* Possibly support side-loading reference information
* Processing of optional attributes

# Support

Feel free to contact me directly if you have any questions about ADAM. My email address is `massie@cs.berkeley.edu`.

