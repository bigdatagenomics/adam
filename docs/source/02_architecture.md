# Architecture {#architecture}

ADAM is architected as an extensible, parallel framework for working with both
aligned and unaligned genomic data using [Apache
Spark](https://spark.apache.org). Unlike traditional genomics tools, ADAM is
built as a modular stack, where a set of schemas define and provide a narrow
waist. This stack architecture enables the support of a wide range of data
formats and optimized query patterns without changing the data structures and
query patterns that users are programming against.

ADAM's architecture was introduced as a response to the challenges
processing the growing volume of genomic sequencing data in a reasonable
timeframe [@schadt10]. While the per-run latency of current genomic
pipelines such as the GATK could be improved by manually partitioning
the input dataset and distributing work, native support for distributed
computing was not provided. As a stopgap solution, projects like
Cloudburst [@schatz09] and Crossbow [@langmead09crossbow]
have ported individual analytics tools to run on top of Hadoop. While
this approach has served well for proofs of concept, this approach provides poor
abstractions for application developers. These poor abstractions make it
difficult for bioinformatics developers to create novel distributed genomic
analyses, and does little to attack sources of inefficiency or incorrectness in
distributed genomics pipelines.

ADAM's architecture reconsiders how we build software for processing
genomic data by eliminating the monolithic architectures that are driven by the
underlying flat file formats used in genomics. These architectures impose
significant restrictions, including:

* These implementations are locked to a single node processing model. Even
  the GATK's "map-reduce" styled walker API [@mckenna10]
  is limited to natively support processing on a single node. While these jobs
  can be manually partitioned and run in a distributed setting, manual
  partitioning can lead to imbalance in work distribution and makes it difficult
  to run algorithms that require aggregating data across all partitions, and
  lacks the fault tolerance provided by modern distributed systems such as
  Apache Hadoop or Spark [@zaharia12].
\item Most of these implementations assume invariants about the sorted
  order of records on disk. This "stack smashing" (specifically, the layout
  of data is used to accelerate a processing stage) can lead to bugs when
  data does not cleanly map to the assumed sort order. Additionally, since these
  sort order invariants are rarely explicit and vary from tool to tool,
  pipelines assembled from disparate tools can be brittle.
\item Additionally, while these invariants are intended to improve performance,
  they do this at the cost of opacity. If we can express the query patterns that
  are accelerated by these invariants at a higher level, then we can achieve
  both a better programming environment and enable various query optimizations.
\end{itemize}

At the core of ADAM, users use the [ADAMContext](#adam-context) to load data as
[GenomicRDDs](#genomic-rdd), which they can then manipulate. In the GenomicRDD class
hierarchy, we provide several classes that contain functionality that is applicable to
all genomic datatypes, such as [coordinate-space joins](#join), the [pipe](#pipes) API,
and genomic metadata management.

## The ADAM Stack Model {#stack-model}

The stack model that ADAM is based upon was introduced in [@massie13] and
further refined in [@nothaft15], and is depicted in the figure below. This
stack model separates computational patterns from the data model, and the
data model from the serialized representation of the data on disk.
This enables developers to write queries that run seamlessly on a single
node or on a distributed cluster, on legacy genomics data files or on
data stored in a high performance columnar storage format, and on sorted or
unsorted data, without making any modifications to their query. Additionally,
this allows developers to write at a higher level of abstraction without
sacrificing performance, since we have the freedom to change the implementation
of a layer of the stack in order to improve the performance of a given query.

![The ADAM Stack Model](source/img/stack-model.pdf)

This stack model is divided into seven levels. Starting from the bottom,
these are:

1. The *physical storage* layer is the type of storage media (e.g., hard
   disk/solid state drives) that are used to store the data.
2. The *data distribution* layer determines how data are made accessible to all
   of the nodes in the cluster. Data may be made accessible through a
   POSIX-compliant shared file system such as NFS [@sandberg85], a non-POSIX
   file system that implements Hadoop's APIs (e.g., HDFS), or through a
   block-store, such as Amazon S3.
3. The *materialized data* layer defines how the logical data in a single
   record maps to bytes on disk. We advocate for the use of [Apache
   Parquet](https://parquet.apache.org), a high performance columnar storage
   format based off of Google's Dremel database [@melnik10]. To support
   conventional genomics file formats, we exchange the Parquet implementation
   of this layer with a layer that maps the given schema into a traditional
   genomics file format (e.g., SAM/BAM/CRAM, BED/GTF/GFF/NarrowPeak, VCF).
4. The *schema* layer defines the logical representation of a datatype.
5. The *evidence access* layer is the software layer that implements and
   executes queries against data stored on disk. While ADAM was originally
   built around Apache Spark's Resilient Distributed Dataset (RDD)
   API [@zaharia12], ADAM has recently enabled the use of Spark
   SQL [@armbrust15] for evidence access and query.
6. The *presentation* layer provides high level abstractions for interacting
   with a parallel collection of genomic data. In ADAM, we implement this layer
   through the [GenomicRDD](#genomic-rdd) classes. This layer presents users
   with a view of the metadata associated with a collection of genomic data,
   and APIs for [transforming](#transforming) and [joining](#join) genomic data.
   Additionally, this is the layer where we provide cross-language support.
7. The *application* layer is the layer where a user writes their application
   on top of the provided APIs.

Our stack model derives its inspiration from the layered Open Systems
Interconnection (OSI) networking stack [@zimmermann80], which eventually served
as the inspiration for the Internet Protocol stack, and from the concept of data
independence in database systems. We see this approach as an alternative to the
"stack smashing" commonly seen in genomics APIs, such as the GATK's "walker"
interface [@mckenna10]. In these APIs, implementation details about the layout
of data on disk (are the data sorted?) are propagated up to the application layer
of the API and exposed to user code. This limits the sorts of queries that can
be expressed efficiently to queries that can easily be run against linear sorted
data. Additionally, these "smashed stacks" typically expose very low level APIs,
such as a sorted iterator, which increases the cost to implementing a query and
can lead to trivial mistakes.

## The bdg-formats schemas {#schemas}

The schemas that comprise ADAM's narrow waist are defined in the
[bdg-formats](https://github.com/bigdatagenomics/bdg-formats) project, using the
[Apache Avro](https://avro.apache.org) schema description language. This schema
definition language automatically generates implementations of this schema for
multiple common languages, including Java, C, C++, and Python. bdg-formats
contains several core schemas:

* The *AlignmentRecord* schema represents a genomic read, along with that read's
  alignment to a reference genome, if available.
* The *Feature* schema represents a generic genomic feature. This record can be
  used to tag a region of the genome with an annotation, such as coverage
  observed over that region, or the coordinates of an exon.
* The *Fragment* schema represents a set of read alignments that came from a
  single sequenced fragment.
* The *Genotype* schema represents a genotype call, along with annotations about
  the quality/read support of the called genotype.
* The *NucleotideContigFragment* schema represents a section of a contig's
  sequence.
* The *Variant* schema represents a sequence variant, along with statistics
  about that variant's support across a group of samples, and annotations about
  the effect of the variant.

The bdg-formats schemas are designed so that common fields are easy to query,
while maintaining extensibility and the ability to interoperate with common
genomics file formats. Where necessary, the bdg-formats schemas are nested,
which allows for the description of complex nested features and groupings (such
as the Fragment record, which groups together AlignmentRecords). All fields in
the bdg-formats schemas are nullable, and the schemas themselves do not contain
invariants around valid values for a field. Instead, we validate data on ingress
and egress to/from a conventional genomic file format. This allows users to take
advantage of features such as field projection, which can improve the
performance of queries like [flagstat](#flagstat) by an order of magnitude.

## Interacting with data through ADAM's evidence access layer {#evidence-access}

ADAM exposes access to distributed datasets of genomic data through the
[ADAMContext](#adam-context) entrypoint. The ADAMContext wraps Apache Spark's
SparkContext, which tracks the configuration and state of the current running
Spark application. On top of the SparkContext, the ADAMContext provides data
loading functions which yield [GenomicRDD](#genomic-rdd)s. The GenomicRDD
classes provide a wrapper around Apache Spark's two APIs for manipulating
distributed datasets: the legacy Resilient Distributed Dataset [@zaharia12] and
the new Spark SQL Dataset/DataFrame API [@armbrust15]. Additionally, the
GenomicRDD is enriched with genomics-specific metadata such as computational
lineage and sample metadata, and optimized genomics-specific query patterns
such as [region joins](#join) and the [auto-parallelizing pipe API](#pipes)
for running legacy tools using Apache Spark.

![The GenomicRDD Class Hierarchy](source/img/grdd.pdf)

All GenomicRDDs include a sequence dictionary which describes the reference genome
that the data in the RDD are aligned to, if one is known. Additionally,
RecordGroupGenomicRDD store a dictionary with read groups that are attached to the
reads/fragments. Similarly, the MultisampleGenomicRDD includes a list of samples who
are present in the dataset.
