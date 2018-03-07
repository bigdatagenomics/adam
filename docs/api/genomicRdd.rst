Working with genomic data using GenomicRDDs
-------------------------------------------

As described in the section on using the
`ADAMContext <adamContext.html>`__, ADAM loads genomic data into a
``GenomicRDD`` which is specialized for each datatype. This
``GenomicRDD`` wraps Apache Spark's Resilient Distributed Dataset (RDD,
(Zaharia et al. 2012)) API with genomic metadata. The ``RDD``
abstraction presents an array of data which is distributed across a
cluster. ``RDD``\ s are backed by a computational lineage, which allows
them to be recomputed if a node fails and the results of a computation
are lost. ``RDD``\ s are processed by running functional
`transformations <#transforming-genomicrdds>`__ across the whole dataset.

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

-  The `core preprocessing <../algorithms/reads.html>`__ algorithms in ADAM:
-  Reads:

   -  Reads to coverage
   -  `Recalibrate base qualities <../algorithms/bqsr.html>`__
   -  `INDEL realignment <../algorithms/ri.html>`__
   -  `Mark duplicate reads <../algorithms/dm.html>`__

-  Fragments:

   -  `Mark duplicate fragments <../algorithms/dm.html>`__

-  `RDD transformations <#transforming-genomicrdds>`__
-  `Spark SQL transformations <#transforming-genomicrdds-via-spark-sql>`__
-  `By using ADAM to pipe out to another tool <pipes.html>`__

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
function required by the `pipe <#pipes.html>`__ API. As an example, let us
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

Spark SQL introduced the strongly-typed
`Dataset API in Spark 1.6.0 <https://spark.apache.org/docs/1.6.0/sql-programming-guide.html#datasets>`__.
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
`ADAMContext <adamContext.html>`__ APIs, this is handled properly; this is
an implementation note necessary only for those bypassing the ADAM APIs.

Similar to ``transform``/``transformDataset``, there exists a
``transmuteDataset`` function that enables transformations between
``GenomicRDD``\ s of different types.

Using partitioned parquet to speed up range based queries  
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
genomicRDDs of types ``AlignmentRecordRDD``, ``GenotypeRDD``, 
``VariantRDD``, and ``NucleotideFragmentContigRDD`` can be written as Parquet 
using a "Hive-style" hierarchical directory scheme based on contig and 
genomic position.  This paritioning reduces the latency of genomic range based
filters against these datasets, particularly important for interactive applications
such as a genomic browser backed by an ADAM dataset.
The genomicRDD function 
``GenomicRDD.filterByOverlappingRegions(queryRegionsList)`` builds a Spark SQL 
query which uses this paritioning scheme to reduce latencies more 
the than 20x when repeatedly querying a datset with genomic range overlap filters.   
On a high coverage alignemnt data we've seen this partitioning strategy improve 
latency from 1-2 minutes to a 1-3 seconds for lookup of genomics ranges.

**Saving partitioned parquet files to disk**  

A ``genomicRDD`` can be written to disk as a 
paritioned parquet dataset with the ``genomicRDD`` function 
``saveAsParitionedParquet``.  The optional ``partitionSize`` parameter 
defines the width in base pairs of the partitions within each contig.
.. code::
 data.saveAsPartitionedParquet("dataset1.adam", partitionSize = 2000000)
A partitioned dataset can also be created from an input ADAM, BAM or VCF files using 
the ADAM transform CLI functions.

**Loading partitioned parquet files**

An genomicRDD can be loaded from a paritioned parquet dataset using the ADAMContext functions 
``loadPartitionedParquetDataset`` or by using the the --isBinned options in the CLI

**Layout of Partitioned Parquet directory**

An ADAM partitioned parquet dataset is written as a three level directory hierarchy.  
The outermost directory is the name of the dataset and contains metadata files as is 
found in unparitioned ADAM parquet datasets.   Wthin the outer dataset directory, 
subdirectories are created with names based on the each of the genomic contigs found 
in the dataset, for example a subdir will be named `contig=\22' for chromosome 22.  
Within each contig directory, there are subdirectories named using a computed value 
``positionBin``, for example a subdirectoy named `positionBin=22'.   Records from the 
dataset are written into  parquet files within each postionBin directory, computed 
based on startPos of the record using the calculation ``floor( start / partitionSize)``.  
For example, when using the default ``partitionSize`` of 1,000,000 base pairs, an 
alignment record with start position 20,100,000 on chromoome 22 would be found in a 
parquet file at the path `mydataset.adam/contig=\22/positionbin=20".   The splitting 
of data into one or more parquet fields in these leaf directories is automatic based on 
parquet block size settings.

.. code:: 

  mySavedAdamDataset.adam
  |
  |-- _partitionedByStartPos
  L-- contig=\1  
      L-- positionBin=\0
          |-- part-r-00001.parquet
          +-- part-r-00002.parquet
      L-- positionBin=\1
          |-- part-r-00003.parquet
          |-- part-r-00004.parquet 
      L-- positionBin= ( N bins ...)              
  L--  contig= ( N contigs ... )
      |-- (N bins ... )
  
  
The existence of the file ``_paritionedByStartPos`` can be tested with the public function ``ADAMContext.isPartitioned(path)`` and can be used to determine explicitly if an ADAM parquet dataset is partitioned using this scheme.  The parition size used when the dataset was written to disk as stored in ``_paritionedByStartPos`` and read as a property of the dataset by the loadPartitionedParquet functions.

The Spark dataset API recognizes that the field `postionBin` is defined implicitly by the parquet files paritioning scheme, and makes 'postionBin' available as a field that can be queried through the Spark SQL API.  `positionBin` is used interally by the public function ``GenomicRDD.filterByOverlappinRegions``.  User code in ADAM-shell or a user applcations could similarly utilize the  `positionBin` field when creating SPAK SQL queries on a ``genomicRDD.dataset`` backed by partitioned parquet.


**Re-using a previously loaded partitioned dataset:**

When a partitioned dataset is first created within an ADAM session a partition discovery/initialization step is performed that can take several minutes for large datasets. 
The original genomicdRDD object can then be re-used multiple times as the parent 
of different filtration and processing transformations and actions, without incurring this initializiation cost again.
Thus, re-use of a parent partitioned ``genomeRDD`` is key to realizing the latency advantages of 
partitioned datasets described above.

.. code:: scala

    val mydata = loadPartitionedParquetAlingments("alignmets.adam")
    val filteredCount1 = mydata.filtermyOverlappingRegions(regions1).dataset.count
    val filteredCount2 = mydata.filtreByOverlappingRegions(regions2).dataset.count










