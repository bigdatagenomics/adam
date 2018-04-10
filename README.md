ADAM
====

[![Build Status](https://img.shields.io/jenkins/s/https/amplab.cs.berkeley.edu/jenkins/view/Big%20Data%20Genomics/job/ADAM.svg)](https://amplab.cs.berkeley.edu/jenkins/view/Big%20Data%20Genomics/job/ADAM/)
[![Coverage Status](https://coveralls.io/repos/github/bigdatagenomics/adam/badge.svg?branch=master)](https://coveralls.io/github/bigdatagenomics/adam?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.bdgenomics.adam/adam-parent-spark2_2.11.svg?maxAge=600)](http://search.maven.org/#search%7Cga%7C1%7Corg.bdgenomics.adam)
[![API Documentation](http://javadoc.io/badge/org.bdgenomics.adam/adam-core-spark2_2.11.svg?color=brightgreen&label=scaladoc)](http://javadoc.io/doc/org.bdgenomics.adam/adam-core-spark2_2.11)

# Introduction

ADAM is a library and command line tool that enables the use of [Apache
Spark](https://spark.apache.org) to parallelize genomic data analysis across
cluster/cloud computing environments. ADAM uses a set of schemas to describe
genomic sequences, reads, variants/genotypes, and features, and can be used
with data in legacy genomic file formats such as SAM/BAM/CRAM, BED/GFF3/GTF,
and VCF, as well as data stored in the columnar
[Apache Parquet](https://parquet.apache.org) format. On a single node, ADAM
provides competitive performance to optimized multi-threaded tools, while
enabling scale out to clusters with more than a thousand cores. ADAM's APIs
can be used from Scala, Java, Python, R, and SQL.

## Why ADAM?

Over the last decade, DNA and RNA sequencing has evolved from an expensive,
labor intensive method to a cheap commodity. The consequence of this is
generation of _massive amounts of genomic and transcriptomic data_. Typically,
tools to process and interpret these data are developed with a focus on
excellence of the results generated, not on __scalability__ and
__interoperability__. A typical _sequencing workflow_ consists of a suite
of tools from quality control, mapping, mapped read preprocessing, to variant
calling or quantification, depending on the application at hand. Concretely,
this usually means that such a workflow is implemented as tools glued together
by scripts or workflow descriptions, with data written to files at each step.
This approach entails three main bottlenecks: 

  1. __scaling the workflow__ comes down to scaling each of the individual
     tools,
  2. the __stability of the workflow__ heavily depends on the consistency of
     intermediate file formats, and
  3. __writing to and reading from disk__ is a major slow-down.

We propose here a transformative solution for these problems, by replacing
ad-hoc workflows by the [ADAM framework](http://bdgenomics.org/), developed
in the [Apache Spark](http://spark.apache.org/) ecosystem.

ADAM enables the high performance in-memory cluster computing functionality
of Apache Spark on genomic data, ensuring efficient and fault-tolerant
distribution based on data parallelism, without the intermediate disk
operations required in traditional distributed approaches.

Furthermore, the ADAM and Apache Spark approach comes with an additional
benefit. Typically, the endpoint of a sequencing pipeline is a file with
processed data for a single sample: e.g. variants for DNA sequencing, read
counts for RNA sequencing, etc. The real endpoint, however, of a sequencing
experiment initiated by an investigator is __interpretation__ of these data
in a certain context. This usually translates into (statistical) analysis of
multiple samples, connection with (clinical) metadata, and interactive
visualization, using data science tools such as R, Python, Tableau and
Spotfire. In addition to scalable distributed processing, Apache Spark also
allows __interactive data analysis__ in the form of analysis notebooks
(Spark Notebook, Jupyter, or Zeppelin), or direct connection to the data in
R and Python.

# Getting Started

## Building from Source

You will need to have [Apache Maven](http://maven.apache.org/) version 3.1.1 or
later installed in order to build ADAM.

> **Note:** The default configuration is for Hadoop 2.7.3. If building against
> a different version of Hadoop, please pass `-Dhadoop.version=<HADOOP_VERSION>`
> to the Maven command.

```bash
$ git clone https://github.com/bigdatagenomics/adam.git
$ cd adam
$ mvn install
```

### Installing Spark

You'll need to have a Spark release on your system and the `$SPARK_HOME` environment variable pointing at it;
prebuilt binaries can be downloaded from the [Spark website](http://spark.apache.org/downloads.html).

# Documentation

ADAM's documentation is available at http://adam.readthedocs.io.

ADAM's core API documentation is available at http://javadoc.io/doc/org.bdgenomics.adam/adam-core-spark2_2.11.

# The ADAM/Big Data Genomics Ecosystem

ADAM builds upon the open source [Apache Spark](https://spark.apache.org),
[Apache Avro](https://avro.apache.org), and [Apache
Parquet](https://parquet.apache.org) projects. Additionally, ADAM can be
deployed for both interactive and production workflows using a variety of
platforms.

There are a number of tools built using ADAM's core APIs:

* [Avocado](https://github.com/bigdatagenomics/avocado) - Avocado is a distributed
  variant caller built on top of ADAM for germline and somatic calling.
* [Cannoli](https://github.com/bigdatagenomics/cannoli) - ADAM
  [Pipe](http://adam.readthedocs.io/en/latest/api/pipes/) API wrappers for bioinformatics
  tools, (e.g.,
  [BWA](https://github.com/lh3/bwa),
  [bowtie2](http://bowtie-bio.sourceforge.net/bowtie2/index.shtml),
  [FreeBayes](https://github.com/ekg/freebayes))
* [DECA](https://github.com/bigdatagenomics/deca) - DECA is a reimplementation of the
  XHMM copy number variant caller on top of ADAM.
* [Gnocchi](https://github.com/bigdatagenomics/gnocchi) - Gnocchi provides primitives
  for running GWAS/eQTL tests on large genotype/phenotype datasets using ADAM.
* [Lime](https://github.com/bigdatagenomics/lime) - Lime provides a
  parallel implementation of genomic set theoretic primitives using the ADAM
  [region join](http://adam.readthedocs.io/en/latest/api/joins/) API.
* [Mango](https://github.com/bigdatagenomics/mango) - Mango is a library for
  visualizing large scale genomics data with interactive latencies.

For more, please see our [awesome list of applications](https://github.com/bigdatagenomics/awesome-adam) that extend ADAM.


# Connecting with the ADAM team

The best way to reach the ADAM team is to post in our [Gitter
channel](https://gitter.im/bigdatagenomics/adam) or to open an issue on our
[Github repository](https://github.com/bigdatagenomics/adam/issues). For more
contact methods, please see [our support page](https://github.com/bigdatagenomics/adam/blob/master/SUPPORT.md).


# License

ADAM is released under the [Apache License, Version 2.0](LICENSE.txt).


# Citing ADAM

ADAM has been described in two manuscripts. The first, [a tech
report](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-207.pdf),
came out in 2013 and described the rationale behind using schemas for genomics,
and presented an early implementation of some of the preprocessing algorithms.
To cite this paper, please cite:

```
@techreport{massie13,
  title={{ADAM}: Genomics Formats and Processing Patterns for Cloud Scale Computing},
  author={Massie, Matt and Nothaft, Frank and Hartl, Christopher and Kozanitis, Christos and Schumacher, Andr{\'e} and Joseph, Anthony D and Patterson, David A},
  year={2013},
  institution={UCB/EECS-2013-207, EECS Department, University of California, Berkeley}
}
```

The second, [a conference paper](http://dl.acm.org/ft_gateway.cfm?ftid=1586788&id=2742787),
appeared in the SIGMOD 2015 Industrial Track. This paper described how ADAM's
design was influenced by database systems, expanded upon the concept of a stack
architecture for scientific analyses, presented more results comparing ADAM to
state-of-the-art single node genomics tools, and demonstrated how the
architecture generalized beyond genomics. To cite this paper, please cite:

```
@inproceedings{nothaft15,
  title={Rethinking Data-Intensive Science Using Scalable Analytics Systems},
  author={Nothaft, Frank A and Massie, Matt and Danford, Timothy and Zhang, Zhao and Laserson, Uri and Yeksigian, Carl and Kottalam, Jey and Ahuja, Arun and Hammerbacher, Jeff and Linderman, Michael and Franklin, Michael and Joseph, Anthony D. and Patterson, David A.},
  booktitle={Proceedings of the 2015 International Conference on Management of Data (SIGMOD '15)},
  year={2015},
  organization={ACM}
}
```

We prefer that you cite both papers, but if you can only cite one paper, we
prefer that you cite the SIGMOD 2015 manuscript.
