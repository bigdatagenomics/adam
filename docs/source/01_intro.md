# Introduction

ADAM is a library and command line tool that enables the use of [Apache
Spark](https://spark.apache.org) to parallelize genomic data analysis across
cluster/cloud computing environments. ADAM uses a set of schemas to describe
genomic sequences, reads, variants/genotypes, and features, and can be used
with data in legacy genomic file formats such as SAM/BAM/CRAM or VCF, as well
as data stored in the columnar [Apache Parquet](https://parquet.apache.org)
format. On a single node, ADAM provides competitive performance to optimized
multi-threaded tools, while enabling scale out to clusters with more than a
thousand cores. ADAM's APIs can be used from Scala, Java, Python, R, and SQL.

## The ADAM/Big Data Genomics Ecosystem

ADAM builds upon the open source [Apache Spark](https://spark.apache.org),
[Apache Avro](https://avro.apache.org), and [Apache
Parquet](https://parquet.apache.org) projects. Additionally, ADAM can be
deployed for both interactive and production workflows using a variety of
platforms. A diagram of the ecosystem of tools and libraries that ADAM builds on
and the tools that build upon the ADAM APIs can be found below.

![The ADAM ecosystem.](source/img/bdgenomics-stack.pdf)

As the diagram shows, beyond the [ADAM CLI](#cli), there are a number of tools
built using ADAM's core APIs:

- [Avocado](https://github.com/bigdatagenomics/avocado) is a variant caller built
  on top of ADAM for germline and somatic calling
- [Cannoli](https://github.com/bigdatagenomics/cannoli) uses ADAM's [pipe](#pipes)
  API to parallelize common single-node genomics tools (e.g.,
  [BWA](https://github.com/lh3/bwa), bowtie,
  [FreeBayes](https://github.com/ekg/freebayes))
- [DECA](https://github.com/bigdatagenomics/deca) is a reimplementation of the
  XHMM copy number variant caller on top of ADAM/Apache Spark
- [Gnocchi](https://github.com/bigdatagenomics/gnocchi) provides primitives for
  running GWAS/eQTL tests on large genotype/phenotype datasets using ADAM
- [Lime](https://github.com/bigdatagenomics/lime) provides a parallel
  implementation of genomic set theoretic primitives using the [region join
  API](#join)
- [Mango](https://github.com/bigdatagenomics/mango) is a library for visualizing
  large scale genomics data with interactive latencies and serving data using the
  [GA4GH schemas](https://github.com/ga4gh/schemas)
