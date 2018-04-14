Architecture Overview
=====================

ADAM is architected as an extensible, parallel framework for working
with both aligned and unaligned genomic data using `Apache
Spark <https://spark.apache.org>`__. Unlike traditional genomics tools,
ADAM is built as a modular stack, where a set of schemas define and
provide a narrow waist. This stack architecture enables the support of a
wide range of data formats and optimized query patterns without changing
the data structures and query patterns that users are programming
against.

ADAM's architecture was introduced as a response to the challenges
processing the growing volume of genomic sequencing data in a reasonable
timeframe (Schadt et al. 2010). While the per-run latency of current
genomic pipelines such as the GATK could be improved by manually
partitioning the input dataset and distributing work, native support for
distributed computing was not provided. As a stopgap solution, projects
like Cloudburst (Schatz 2009) and Crossbow (Langmead et al. 2009) have
ported individual analytics tools to run on top of Hadoop. While this
approach has served well for proofs of concept, this approach provides
poor abstractions for application developers. These poor abstractions
make it difficult for bioinformatics developers to create novel
distributed genomic analyses, and does little to attack sources of
inefficiency or incorrectness in distributed genomics pipelines.

ADAM's architecture reconsiders how we build software for processing
genomic data by eliminating the monolithic architectures that are driven
by the underlying flat file formats used in genomics. These
architectures impose significant restrictions, including:

-  These implementations are locked to a single node processing model.
   Even the GATK's "map-reduce" styled walker API (McKenna et al. 2010)
   is limited to natively support processing on a single node. While
   these jobs can be manually partitioned and run in a distributed
   setting, manual partitioning can lead to imbalance in work
   distribution and makes it difficult to run algorithms that require
   aggregating data across all partitions, and lacks the fault tolerance
   provided by modern distributed systems such as Apache Hadoop or Spark
   (Zaharia et al. 2012).
-  Most of these implementations assume
   invariants about the sorted order of records on disk. This "stack
   smashing" (specifically, the layout of data is used to accelerate a
   processing stage) can lead to bugs when data does not cleanly map to
   the assumed sort order. Additionally, since these sort order
   invariants are rarely explicit and vary from tool to tool, pipelines
   assembled from disparate tools can be brittle.
-  Additionally,
   while these invariants are intended to improve performance, they do
   this at the cost of opacity. If we can express the query patterns
   that are accelerated by these invariants at a higher level, then we
   can achieve both a better programming environment and enable various
   query optimizations.

At the core of ADAM, users use the `ADAMContext <../api/adamContext.html>`__ to
load data as `GenomicDatasets <../api/genomicDataset.html>`__, which they can then
manipulate. In the GenomicDataset class hierarchy, we provide several
classes that contain functionality that is applicable to all genomic
datatypes, such as `coordinate-space joins <../api/joins.html>`__, the
`pipe <../api/pipes.html>`__ API, and genomic metadata management.
