Conversion tools
----------------

These tools convert data between a legacy genomic file format and using
ADAM's schemas to store data in Parquet.

fasta2adam and adam2fasta
~~~~~~~~~~~~~~~~~~~~~~~~~

These commands convert between FASTA and Parquet files storing
assemblies using the NucleotideContigFragment schema.

``fasta2adam`` takes two required arguments:

1. ``FASTA``: The input FASTA file to convert.
2. ``ADAM``: The path to save the Parquet formatted
   NucleotideContigFragments to.

``fasta2adam`` supports the full set of `default
options <#default-args>`__, as well as the following options:

-  ``-fragment_length``: The fragment length to shard a given contig
   into. Defaults to 10,000bp.
-  ``-reads``: Path to a set of reads that includes sequence info. This
   read path is used to obtain the sequence indices for ordering the
   contigs from the FASTA file.
-  ``-repartition``: The number of partitions to save the data to. If
   provided, forces a shuffle.
-  ``-verbose``: If given, enables additional logging where the sequence
   dictionary is printed.

``adam2fasta`` takes two required arguments:

1. ``ADAM``: The path to a Parquet file containing
   NucleotideContigFragments.
2. ``FASTA``: The path to save the FASTA file to.

``adam2fasta`` only supports the ``-print_metrics`` option from the
`default options <#default-args>`__. Additionally, ``adam2fasta`` takes
the following options:

-  ``-line_width``: The line width in characters to use for breaking
   FASTA lines. Defaults to 60 characters.
-  ``-coalesce``: Sets the number of partitions to coalesce the output
   to. If ``-force_shuffle_coalesce`` is not provided, the Spark engine
   may ignore the coalesce directive.
-  ``-force_shuffle_coalesce``: Forces a shuffle that leads to the
   output being saved with the number of partitions requested by
   ``-coalesce``. This is necessary if the ``-coalesce`` would increase
   the number of partitions, or if it would reduce the number of
   partitions to fewer than the number of Spark executors. This may have
   a substantial performance cost, and will invalidate any sort order.

adam2fastq
~~~~~~~~~~

While the ```transformAlignments`` <#transformAlignments>`__ command can
export to FASTQ, the ``adam2fastq`` provides a simpler CLI with more
output options. ``adam2fastq`` takes two required arguments and an
optional third argument:

1. ``INPUT``: The input read file, in any ADAM-supported read format.
2. ``OUTPUT``: The path to save an unpaired or interleaved FASTQ file
   to, or the path to save the first-of-pair reads to, for paired FASTQ.
3. Optional ``SECOND_OUTPUT``: If saving paired FASTQ, the path to save
   the second-of-pair reads to.

``adam2fastq`` only supports the ``-print_metrics`` option from the
`default options <#default-args>`__. Additionally, ``adam2fastq`` takes
the following options:

-  ``-no_projection``: By default, ``adam2fastq`` only projects the
   fields necessary for saving to FASTQ. This option disables that
   projection and projects all fields.
-  ``-output_oq``: Outputs the original read qualities, if available.
-  ``-persist_level``: Sets the Spark `persistence
   level <http://spark.apache.org/docs/latest/programming-guide.html#which-storage-level-to-choose>`__
   for cached data during the conversion back to FASTQ. If not provided,
   the intermediate RDDs are not cached.
-  ``-repartition``: The number of partitions to save the data to. If
   provided, forces a shuffle.
-  ``-validation``: Sets the validation stringency for checking whether
   reads are paired when saving paired reads. Defaults to ``LENIENT.``
   See `validation stringency <#validation>`__ for more details.

transformFragments
~~~~~~~~~~~~~~~~~~

These two commands translate read data between the single read alignment
and fragment representations.

``transformFragments`` takes two required arguments:

1. ``INPUT``: The input fragment file, in any ADAM-supported read or
   fragment format.
2. ``OUTPUT``: The path to save reads at, in any ADAM-supported read or
   fragment format.

``transformFragments`` takes the `default options <#default-args>`__.
Additionally, ``transformFragments`` takes the following options:

-  ``-mark_duplicate_reads``: Marks reads as fragment duplicates.
   Running mark duplicates on fragments improves performance by
   eliminating one ``groupBy`` (and therefore, a shuffle) versus running
   on reads.
-  Base quality binning: If the ``-bin_quality_scores`` option is
   passed, the quality scores attached to the reads will be rewritten
   into bins. This option takes a semicolon (``;``) delimited list,
   where each element describes a bin. The description for a bin is
   three integers: the bottom of the bin, the top of the bin, and the
   value to assign to bases in the bin. E.g., given the description
   ``0,20,10:20,50,30``, all quality scores between 0–19 will be
   rewritten to 10, and all quality scores between 20–49 will be
   rewritten to 30.
-  ``-load_as_reads``: Treats the input as a read file (uses
   ``loadAlignments`` instead of ``loadFragments``), which behaves
   differently for unpaired FASTQ.
-  ``-save_as_reads``: Saves the output as a Parquet file of
   ``AlignmentRecord``\ s, as SAM/BAM/CRAM, or as FASTQ, depending on
   the output file extension. If this option is specified, the output
   can also be sorted:
-  ``-sort_reads``: Sorts reads by alignment position. Unmapped reads
   are placed at the end of all reads. Contigs are ordered by sequence
   record index.
-  ``-sort_lexicographically``: Sorts reads by alignment position.
   Unmapped reads are placed at the end of all reads. Contigs are
   ordered lexicographically.

