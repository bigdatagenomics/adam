Working with genomic data using GenomicRDDs
-------------------------------------------

As described in the section on using the
`ADAMContext <#adam-context>`__, ADAM loads genomic data into a
``GenomicRDD`` which is specialized for each datatype. This
``GenomicRDD`` wraps Apache Spark's Resilient Distributed Dataset (RDD,
(Zaharia et al. 2012)) API with genomic metadata. The ``RDD``
abstraction presents an array of data which is distributed across a
cluster. ``RDD``\ s are backed by a computational lineage, which allows
them to be recomputed if a node fails and the results of a computation
are lost. ``RDD``\ s are processed by running functional
[transformations]{#transforming} across the whole dataset.

Around an ``RDD``, ADAM adds metadata which describes the genome,
samples, or read group that a dataset came from. Specifically, ADAM
supports the following metadata:

-  ``GenomicRDD`` base: A sequence dictionary, which describes the
   reference assembly that data are aligned to, if it is aligned.
   Applies to all types.
-  ``MultisampleGenomicRDD``: Adds metadata about the samples in a
   dataset. Applies to ``GenotypeRDD``.
-  ``ReadGroupGenomicRDD``: Adds metadata about the read groups attached
   to a dataset. Applies to ``AlignmentRecordRDD`` and ``FragmentRDD``.

Additionally, ``GenotypeRDD``, ``VariantRDD``, and ``VariantContextRDD``
store the VCF header lines attached to the original file, to enable a
round trip between Parquet and VCF.

``GenomicRDD``\ s can be transformed several ways. These include:

-  The `core preprocessing <#algorithms>`__ algorithms in ADAM:
-  Reads:

   -  Reads to coverage
   -  `Recalibrate base qualities <#bqsr>`__
   -  `INDEL realignment <#realignment>`__
   -  `Mark duplicate reads <#duplicate-marking>`__

-  Fragments:

   -  `Mark duplicate fragments <#duplicate-marking>`__

-  `RDD transformations <#transforming>`__
-  `Spark SQL transformations <#sql>`__
-  `By using ADAM to pipe out to another tool <#pipes>`__

Transforming GenomicRDDs
~~~~~~~~~~~~~~~~~~~~~~~~

Although ``GenomicRDD``\ s do not extend Apache Spark's ``RDD`` class,
``RDD`` operations can be performed on them using the ``transform``
method. Currently, we only support ``RDD`` to ``RDD`` transformations
that keep the same type as the base type of the ``GenomicRDD``. To apply
an ``RDD`` transform, use the ``transform`` method, which takes a
function mapping one ``RDD`` of the base type into another ``RDD`` of
the base type. For example, we could use ``transform`` on an
``AlignmentRecordRDD`` to filter out reads that have a low mapping
quality, but we cannot use ``transform`` to translate those reads into
``Feature``\ s showing the genomic locations covered by reads.

If we want to transform a ``GenomicRDD`` into a new ``GenomicRDD`` that
contains a different datatype (e.g., reads to features), we can instead
use the ``transmute`` function. The ``transmute`` function takes a
function that transforms an ``RDD`` of the type of the first
``GenomicRDD`` into a new ``RDD`` that contains records of the type of
the second ``GenomicRDD``. Additionally, it takes an implicit function
that maps the metadata in the first ``GenomicRDD`` into the metadata
needed by the second ``GenomicRDD``. This is akin to the implicit
function required by the `pipe <#pipes>`__ API. As an example, let us
use the ``transmute`` function to make features corresponding to reads
containing INDELs:

.. code:: scala

    // pick up implicits from ADAMContext
    import org.bdgenomics.adam.rdd.ADAMContext._

    val reads = sc.loadAlignments("path/to/my/reads.adam")

    // the type of the transmuted RDD normally needs to be specified
    // import the FeatureRDD, which is the output type
    import org.bdgenomics.adam.rdd.feature.FeatureRDD
    import org.bdgenomics.formats.avro.Feature

    val features: FeatureRDD = reads.transmute(rdd => {
      rdd.filter(r => {
        // does the CIGAR for this read contain an I or a D?
        Option(r.getCigar)
          .exists(c => c.contains("I") || c.contains("D"))
      }).map(r => {
        Feature.newBuilder
          .setContigName(r.getContigName)
          .setStart(r.getStart)
          .setEnd(r.getEnd)
          .build
      })
    })

``ADAMContext`` provides the implicit functions needed to run the
``transmute`` function between all ``GenomicRDD``\ s contained within
the ``org.bdgenomics.adam.rdd`` package hierarchy. Any custom
``GenomicRDD`` can be supported by providing a user defined conversion
function.

Transforming GenomicRDDs via Spark SQL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Spark SQL introduced the strongly-typed ```Dataset`` API in Spark
1.6.0 <https://spark.apache.org/docs/1.6.0/sql-programming-guide.html#datasets>`__.
This API supports seamless translation between the RDD API and a
strongly typed DataFrame style API. While Spark SQL supports many types
of encoders for translating data from an RDD into a Dataset, no encoders
support the Avro models used by ADAM to describe our genomic schemas. In
spite of this, Spark SQL is highly desirable because it has a more
efficient execution engine than the Spark RDD APIs, which can lead to
substantial speedups for certain queries.

To resolve this, we added an ``adam-codegen`` package that generates
Spark SQL compatible classes representing the ADAM schemas. These
classes are available in the ``org.bdgenomics.adam.sql`` package. All
Avro-backed GenomicRDDs now support translation to Datasets via the
``dataset`` field, and transformation via the Spark SQL APIs through the
``transformDataset`` method. As an optimization, we lazily choose either
the RDD or Dataset API depending on the calculation being performed. For
example, if one were to load a Parquet file of reads, we would not
decide to load the Parquet file as an RDD or a Dataset until we saw your
query. If you were to load the reads from Parquet and then were to
immediately run a ``transformDataset`` call, it would be more efficient
to load the data directly using the Spark SQL APIs, instead of loading
the data as an RDD, and then transforming that RDD into a SQL Dataset.

The functionality of the ``adam-codegen`` package is simple. The goal of
this package is to take ADAM's Avro schemas and to remap them into
classes that implement Scala's ``Product`` interface, and which have a
specific style of constructor that is expected by Spark SQL.
Additionally, we define functions that translate between these Product
classes and the bdg-formats Avro models. Parquet files written with
either the Product classes and Spark SQL Parquet writer or the Avro
classes and the RDD/ParquetAvroOutputFormat are equivalent and can be
read through either API. However, to support this, we must explicitly
set the requested schema on read when loading data through the RDD read
path. This is because Spark SQL writes a Parquet schema that is
equivalent but not strictly identical to the Parquet schema that the
Avro/RDD write path writes. If the schema is not set, then schema
validation on read fails. If reading data using the
`ADAMContext <#adam-context>`__ APIs, this is handled properly; this is
an implementation note necessary only for those bypassing the ADAM APIs.

Similar to ``transform``/``transformDataset``, there exists a
``transmuteDataset`` function that enables transformations between
``GenomicRDD``\ s of different types.

