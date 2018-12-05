Loading data with the ADAMContext
---------------------------------

The ADAMContext is the main entrypoint to using ADAM. The ADAMContext
wraps an existing
`SparkContext <http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext>`__
to provide methods for loading genomic data. In Scala, we provide an
implicit conversion from a ``SparkContext`` to an ``ADAMContext``. To
use this, import the implicit, and call an ``ADAMContext`` method:

.. code:: scala

    import org.apache.spark.SparkContext

    // the ._ at the end imports the implicit from the ADAMContext companion object
    import org.bdgenomics.adam.rdd.ADAMContext._
    import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset

    def loadReads(filePath: String, sc: SparkContext): AlignmentRecordDataset = {
      sc.loadAlignments(filePath)
    }

In Java, instantiate a JavaADAMContext, which wraps an ADAMContext:

.. code:: java

    import org.apache.spark.apis.java.JavaSparkContext;
    import org.bdgenomics.adam.apis.java.JavaADAMContext;
    import org.bdgenomics.adam.rdd.ADAMContext;
    import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset;

    class LoadReads {

      public static AlignmentRecordDataset loadReads(String filePath,
                                                     JavaSparkContext jsc) {
        // create an ADAMContext first
        ADAMContext ac = new ADAMContext(jsc.sc());

        // then wrap that in a JavaADAMContext
        JavaADAMContext jac = new JavaADAMContext(ac);

        return jac.loadAlignments(filePath);
      }
    }

From Python, instantiate an ADAMContext, which wraps a SparkContext:

.. code:: python

    from bdgenomics.adam.adamContext import ADAMContext

    ac = ADAMContext(sc)

    reads = ac.loadAlignments("my/read/file.adam")

With an ``ADAMContext``, you can load:

-  Single reads as an ``AlignmentRecordDataset``:

   -  From SAM/BAM/CRAM using ``loadBam`` (Scala only)
   -  Selected regions from an indexed BAM/CRAM using ``loadIndexedBam`` (Scala, Java, and Python)
   -  From FASTQ using ``loadFastq``, ``loadPairedFastq``, and ``loadUnpairedFastq`` (Scala only)
   -  From Parquet using ``loadParquetAlignments`` (Scala only)
   -  From partitioned Parquet using ``loadPartitionedParquetAlignments`` (Scala only)
   -  The ``loadAlignments`` method will load from any of the above formats, and will autodetect the
      underlying format (Scala, Java, Python, and R, also supports loading reads from FASTA)

-  Paired reads as a ``FragmentDataset``:

   -  From interleaved FASTQ using ``loadInterleavedFastqAsFragments`` (Scala only)
   -  From Parquet using ``loadParquetFragments`` (Scala only)
   -  The ``loadFragments`` method will load from either of the above formats, as well as SAM/BAM/CRAM,
      and will autodetect the underlying file format. If the file is a SAM/BAM/CRAM file and the file is
      queryname sorted, the data will be converted to fragments without performing a shuffle. (Scala, Java, Python, and R)

-  All of the genotypes associated with a variant as a ``VariantContextDataset`` from Parquet
   using ``loadParquetVariantContexts`` (Scala only)
-  VCF lines as a ``VariantContextDataset`` from VCF/BCF1 using ``loadVcf`` (Scala only)
-  Selected lines from a tabix indexed VCF using ``loadIndexedVcf`` (Scala only)
-  Genotypes as a ``GenotypeDataset``:

   -  From Parquet using ``loadParquetGenotypes`` (Scala only)
   -  From partitioned Parquet using ``loadPartitionedParquetGenotypes`` (Scala only)
   -  From either Parquet or VCF/BCF1 using ``loadGenotypes`` (Scala, Java, Python, and R)

-  Variants as a ``VariantDataset``:

   -  From Parquet using ``loadParquetVariants`` (Scala only)
   -  From partitioned Parquet using ``loadPartitionedParquetVariants`` (Scala only)
   -  From either Parquet or VCF/BCF1 using ``loadVariants`` (Scala, Java, Python, and R)

-  Genomic features as a ``FeatureDataset``:

   -  From BED using ``loadBed`` (Scala only)
   -  From GFF3 using ``loadGff3`` (Scala only)
   -  From GFF2/GTF using ``loadGtf`` (Scala only)
   -  From NarrowPeak using ``loadNarrowPeak`` (Scala only)
   -  From IntervalList using ``loadIntervalList`` (Scala only)
   -  From Parquet using ``loadParquetFeatures`` (Scala only)
   -  From partitioned Parquet using ``loadPartitionedParquetFeatures`` (Scala only)
   -  Autodetected from any of the above using ``loadFeatures`` (Scala, Java, Python, and R)

-  Fragmented contig sequence as a ``NucleotideContigFragmentDataset``:

   -  From FASTA with ``loadFasta`` (Scala only)
   -  From Parquet with ``loadParquetContigFragments`` (Scala only)
   -  From partitioned Parquet with ``loadPartitionedParquetContigFragments`` (Scala only)
   -  Autodetected from either of the above using ``loadSequences`` (Scala, Java, Python, and R)

-  Coverage data as a ``CoverageDataset``:

   -  From Parquet using ``loadParquetCoverage`` (Scala only)
   -  From Parquet or any of the feature file formats using ``loadCoverage`` (Scala only)
   -  Contig sequence as a broadcastable ``ReferenceFile`` using ``loadReferenceFile``, which supports
      2bit files, FASTA, and Parquet (Scala only)

The methods labeled "Scala only" may be usable from Java, but may not be
convenient to use.

The ``JavaADAMContext`` class provides Java-friendly methods that are
equivalent to the ``ADAMContext`` methods. Specifically, these methods
use Java types, and do not make use of default parameters. In addition
to the load/save methods described above, the ``ADAMContext`` adds the
implicit methods needed for using ADAM's `pipe <pipes.html>`__ API.
