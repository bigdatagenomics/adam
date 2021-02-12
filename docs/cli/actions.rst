Action tools
------------

Roughly speaking, "action" tools apply some form of non-trivial
transformation to data using the ADAM APIs.

countKmers and countContigKmers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Counts the `k` length substrings in either a set of reads or
reference fragments. Takes three required arguments:

1. ``INPUT``: The input path. A set of reads for ``countKmers`` or a set
   of reference contigs for ``countContigKmers``.
2. ``OUTPUT``: The path to save the output to. Saves the output as
   `CSV <https://en.wikipedia.org/wiki/Comma-separated_values>`__
   containing the `k`-mer sequence and count.
3. ``KMER_LENGTH``: The length `k` of substrings to count.

Beyond the `default options <#default-args>`__, both ``countKmers`` and
``countContigKmers`` take one option:

-  ``-print_histogram``: If provided, prints a histogram of the
   `k`-mer count distribution to standard out.

transformAlignments
~~~~~~~~~~~~~~~~~~~

The ``transformAlignments`` CLI is the entrypoint to ADAM's read
preprocessing tools. This command provides drop-in replacement commands
for several commands in the `Genome Analysis
Toolkit <https://software.broadinstitute.org/gatk/>`__ "Best Practices"
read preprocessing pipeline and more (DePristo et al. 2011). This CLI
tool takes two required arguments:

1. ``INPUT``: The input path. A file containing reads in any of the
   supported ADAM read input formats.
2. ``OUTPUT``: The path to save the transformed reads to. Supports any
   of ADAM's read output formats.

Beyond the `default options <#default-args>`__, the `legacy output
options <#legacy-output>`__, and the
`partitioned output options <#partitioned-output>`__,
``transformAlignments`` supports a vast range of options. These options
fall into several general categories:

-  General options:

   -  ``-cache``: If provided, the results of intermediate stages will
      be cached. This is necessary to avoid recomputation if running
      multiple transformations (e.g., Indel realignment, BQSR, etc) back
      to back.
   -  ``-storage_level``: Along with ``-cache``, this can be used to set
      the Spark `persistance
      level <http://spark.apache.org/docs/latest/programming-guide.html#which-storage-level-to-choose>`__
      for cached data. If not provided, this defaults to ``MEM_ONLY``.
   -  ``-stringency``: Sets the validation stringency for various
      operations. Defaults to ``LENIENT.`` See `validation
      stringency <#validation>`__ for more details.

-  Loading options:

   -  ``-repartition``: Forces a repartition on load. Useful to increase
      the available parallelism on a small dataset. Forces a shuffle.
      Takes the number of partitions to repartition to.
   -  ``-force_load_bam``: Forces ADAM to try to load the input as
      SAM/BAM/CRAM.
   -  ``-force_load_fastq``: Forces ADAM to try to load the input as
      FASTQ.
   -  ``-paired_fastq``: Forces ``-force_load_fastq``, and passes the
      path of a second-of-pair FASTQ file to load.
   -  ``-read_group``: If loading FASTQ, sets the read group name on
      each read to this value.
   -  ``-force_load_ifastq``: Forces ADAM to try to load the input as
      interleaved FASTQ.
   -  ``-force_load_parquet``: Forces ADAM to try to load the input as
      Parquet encoded using the ADAM ``Alignment`` schema.
   -  ``-limit_projection``: If loading as Parquet, sets a projection
      that does not load the ``attributes`` or ``origQual`` fields of
      the ``Alignment``.
   -  ``-aligned_read_predicate``: If loading as Parquet, only loads
      aligned reads.
   -  ``-region_predicate``: A string indicating that reads should be
      filtered on overlapping a genomic position or range. This argument
      takes a comma separated list, where each element in the list takes
      the form:
   -  ``contig:pos`` for a single genomic position, or
   -  ``contig:start-end`` for a genomic range with closed start and
      open end E.g., ``-region_predicate 1:100,2:1000-2000`` would
      filter all reads that overlapped either position 100 on ``1`` or
      the range from 1,000 to 2,000 on ``2``.
   -  ``-concat``: Provides a path to an optional second file to load,
      which is then concatenated to the file given as the ``INPUT``
      path.

-  Duplicate marking options: Duplicate marking is run with the
   ``-mark_duplicate_reads`` option. It takes no optional parameters.
-  BQSR options: BQSR is run with the ``-recalibrate_base_qualities``
   flag. Additionally, the BQSR engine takes the following parameters:

   -  ``-known_snps``: Path to a VCF file/Parquet variant file
      containing known point variants. These point variants are used to
      mask read errors during recalibration. Specifically, putative read
      errors that are at variant sites are treated as correct
      observations. If BQSR is run, this option should be passed, along
      with a path to a known variation database (e.g.,
      `dbSNP <https://www.ncbi.nlm.nih.gov/projects/SNP/>`__).
   -  ``-sampling_fraction``: The fraction of reads to sample when creating
      the recalibration table. Omitted by default.
   -  ``-sampling_seed``: The seed to use when randomly sampling reads. Only
      used if ``-sampling_fraction`` is given. We do not set this parameter
      by default, which can lead to inconsistent results on a run-by-run
      basis due to randomization. If the sampling seed is set or sampling
      is omitted, then two runs will produce equivalent results. If this
      parameter is set to 0, then a seed will not be used.

-  Indel realignment options: Indel realignment is run with the
   ``-realign_indels`` flag. Additionally, the Indel realignment engine
   takes the following options:

   -  ``-known_indels``: Path to a VCF file/Parquet variant file
      containing known Indel variants to realign against. If provided,
      forces the ``KNOWNS_ONLY`` consensus model. If not provided,
      forces the ``CONSENSUS_FROM_READS`` model. See `candidate
      generation and realignment <#consensus-model>`__.
   -  ``-max_consensus_number``: The maximum number of consensus
      sequences to realign a single target against. If more consensus
      sequences are seen at a single target, we randomly downsample.
      Defaults to 30.
   -  ``-max_indel_size``: The maximum length of an Indel to realign
      against. Indels longer than this size are dropped before
      generating consensus sequences. Defaults to 500bp.
   -  ``-max_target_size``: The maximum length of a target to realign.
      Targets longer than this size are dropped before trying to
      realign. Defaults to 3,000bp.
   -  ``-max_reads_per_target``: The maximum number of reads in a target
      that we will try to realign. By default, this value is 20,000
      reads. If we encounter a target with more than this number of
      reads, we skip realigning this target.
   -  ``-reference``: An optional path to a reference genome assembly
      build. If this is not provided, then we attempt to reconstruct the
      reference at each target from the MD tags on each read.
   -  ``-unclip_reads``: If provided, we will unclip reads when
      attempting to realign them to the reference. If not provided, we
      leave clipped reads clipped.
   -  ``-log_odds_threshold``: The log odds threshold to use for picking
      a consensus sequence to finalize realignments against. A consensus
      will not be realigned against unless the Phred weighted edit
      distance against the given consensus/reference pair is a
      sufficient improvement over the original reference realignments.
      This option sets that improvement weight. Defaults to 5.0.

-  Base quality binning: If the ``-bin_quality_scores`` option is
   passed, the quality scores attached to the reads will be rewritten
   into bins. This option takes a semicolon (``;``) delimited list,
   where each element describes a bin. The description for a bin is
   three integers: the bottom of the bin, the top of the bin, and the
   value to assign to bases in the bin. E.g., given the description
   ``0,20,10:20,50,30``, all quality scores between 0–19 will be
   rewritten to 10, and all quality scores between 20–49 will be
   rewritten to 30.
-  ``mismatchingPositions`` tagging options: We can recompute the
   ``mismatchingPositions`` field of an Alignment (SAM "MD" tag)
   with the ``-add_md_tags`` flag. This flag takes a path to a reference
   file in either FASTA or Parquet ``Sequence`` format.
   Additionally, this engine takes the following options:

   -  ``-md_tag_fragment_size``: If loading from FASTA, sets the size of
      each fragment to load. Defaults to 10,000bp.
   -  ``-md_tag_overwrite``: If provided, recomputes and overwrites the
      ``mismatchingPositions`` field for records where this field was
      provided.

-  Output options: ``transformAlignments`` supports the `legacy
   output <#legacy-output>`__ options. Additionally, there are the
   following options:

   -  ``-coalesce``: Sets the number of partitions to coalesce the
      output to. If ``-force_shuffle_coalesce`` is not provided, the
      Spark engine may ignore the coalesce directive.
   -  ``-force_shuffle_coalesce``: Forces a shuffle that leads to the
      output being saved with the number of partitions requested by
      ``-coalesce``. This is necessary if the ``-coalesce`` would
      increase the number of partitions, or if it would reduce the
      number of partitions to fewer than the number of Spark executors.
      This may have a substantial performance cost, and will invalidate
      any sort order.
   -  ``-sort_by_read_name``: Sorts alignments by read name.
   -  ``-sort_by_reference_position``: Sorts alignments by the location
      where the reads are aligned. Unaligned reads are put at the end and
      sorted by read name. References are ordered lexicographically.
   -  ``-sort_by_reference_position_and_index``: Sorts alignments by the
      location where the reads are aligned. Unaligned reads are put at the
      end and sorted by read name. References are ordered by index that they
      are ordered in the reference SequenceDictionary.
   -  ``-sort_fastq_output``: Ignored if not saving to FASTQ. If saving
      to FASTQ, sorts the output reads by read name.

transformFeatures
~~~~~~~~~~~~~~~~~

Loads a feature file into the ADAM ``Feature`` schema, and saves it
back. The input and output formats are autodetected. Takes two required
arguments:

1. ``INPUT``: The input path. A file containing features in any of the
   supported ADAM feature input formats.
2. ``OUTPUT``: The path to save the transformed features to. Supports
   any of ADAM's feature output formats.

Beyond the `default options <#default-args>`__ and the `legacy output
options <#legacy-output>`__, ``transformFeatures`` has
one optional argument:

-  ``-num_partitions``: If loading from a textual feature format (i.e.,
   not Parquet), sets the number of partitions to load. If not provided,
   this is chosen by Spark.

transformGenotypes
~~~~~~~~~~~~~~~~~~

Loads a genotype file into the ADAM ``Genotype`` schema, and saves it
back. The input and output formats are autodetected. Takes two required
arguments:

1. ``INPUT``: The input path. A file containing genotypes in any of the
   supported ADAM genotype input formats.
2. ``OUTPUT``: The path to save the transformed genotypes to. Supports
   any of ADAM's genotype output formats.

Beyond the `default options <#default-args>`__, the `legacy output
options <#legacy-output>`__, and the
`partitioned output options <#partitioned-output>`__, ``transformGenotypes``
has additional arguments:

-  ``-coalesce``: Sets the number of partitions to coalesce the output
   to. If ``-force_shuffle_coalesce`` is not provided, the Spark engine
   may ignore the coalesce directive.
-  ``-force_shuffle_coalesce``: Forces a shuffle that leads to the
   output being saved with the number of partitions requested by
   ``-coalesce``. This is necessary if the ``-coalesce`` would increase
   the number of partitions, or if it would reduce the number of
   partitions to fewer than the number of Spark executors. This may have
   a substantial performance cost, and will invalidate any sort order.
-  ``-sort_on_save``: Sorts the genotypes when saving, where contigs are
   ordered by sequence index. Conflicts with
   ``-sort_lexicographically_on_save``.
-  ``-sort_lexicographically_on_save``: Sorts the genotypes when saving,
   where contigs are ordered lexicographically. Conflicts with
   ``-sort_on_save``.
-  ``-single``: Saves the VCF file as headerless shards, and then merges
   the sharded files into a single VCF.
-  ``-stringency``: Sets the validation stringency for conversion.
   Defaults to ``LENIENT.`` See `validation stringency <#validation>`__
   for more details.

In this command, the validation stringency is applied to the individual
genotypes. If a genotype fails validation, the individual genotype will
be dropped (for lenient or silent validation, under strict validation,
conversion will fail). Header lines are not validated. Due to a
constraint imposed by the
`htsjdk <https://github.com/samtools/htsjdk>`__ library, which we use to
parse VCF files, user provided header lines that do not match the header
line definitions from the `VCF
4.2 <https://samtools.github.io/hts-specs/VCFv4.2.png>`__ spec will be
overridden with the line definitions from the specification.
Unfortunately, this behavior cannot be disabled. If there is a user
provided vs. spec mismatch in format/info field count or type, this will
likely cause validation failures during conversion.

transformVariants
~~~~~~~~~~~~~~~~~

Loads a variant file into the ADAM ``Variant`` schema, and saves it
back. The input and output formats are autodetected. Takes two required
arguments:

1. ``INPUT``: The input path. A file containing variants in any of the
   supported ADAM variant input formats.
2. ``OUTPUT``: The path to save the transformed variants to. Supports
   any of ADAM's variant output formats.

Beyond the `default options <#default-args>`__ and the `legacy output
options <#legacy-output>`__, ``transformVariants`` has
additional arguments:

-  ``-coalesce``: Sets the number of partitions to coalesce the output
   to. If ``-force_shuffle_coalesce`` is not provided, the Spark engine
   may ignore the coalesce directive.
-  ``-force_shuffle_coalesce``: Forces a shuffle that leads to the
   output being saved with the number of partitions requested by
   ``-coalesce``. This is necessary if the ``-coalesce`` would increase
   the number of partitions, or if it would reduce the number of
   partitions to fewer than the number of Spark executors. This may have
   a substantial performance cost, and will invalidate any sort order.
-  ``-sort_on_save``: Sorts the variants when saving, where contigs are
   ordered by sequence index. Conflicts with
   ``-sort_lexicographically_on_save``.
-  ``-sort_lexicographically_on_save``: Sorts the variants when saving,
   where contigs are ordered lexicographically. Conflicts with
   ``-sort_on_save``.
-  ``-single``: Saves the VCF file as headerless shards, and then merges
   the sharded files into a single VCF.
-  ``-stringency``: Sets the validation stringency for conversion.
   Defaults to ``LENIENT.`` See `validation stringency <#validation>`__
   for more details.

In this command, the validation stringency is applied to the individual
variants. If a variant fails validation, the individual variant will be
dropped (for lenient or silent validation, under strict validation,
conversion will fail). Header lines are not validated. Due to a
constraint imposed by the
`htsjdk <https://github.com/samtools/htsjdk>`__ library, which we use to
parse VCF files, user provided header lines that do not match the header
line definitions from the `VCF
4.2 <https://samtools.github.io/hts-specs/VCFv4.2.png>`__ spec will be
overridden with the line definitions from the specification.
Unfortunately, this behavior cannot be disabled. If there is a user
provided vs. spec mismatch in format/info field count or type, this will
likely cause validation failures during conversion.

mergeShards
~~~~~~~~~~~

A CLI tool for merging a `sharded legacy file <#legacy-output>`__ that
was written with the ``-single`` and ``-defer_merging`` flags. Runs the
file merging process. Takes two required arguments:

1. ``INPUT``: The input directory of sharded files to merge.
2. ``OUTPUT``: The path to save the merged file at.

This command takes several optional arguments:

-  ``-buffer_size``: The buffer size in bytes to use for copying data on
   the driver. Defaults to 4MB (4 \* 1024 \* 1024).
-  ``-header_path``: The path to a header file that should be written to
   the start of the merged output.
-  ``-write_cram_eof``: Writes an empty CRAM container at the end of the
   merged output file. This should not be provided unless merging a
   sharded CRAM file.
-  ``-write_empty_GZIP_at_eof``: Writes an empty GZIP block at the end
   of the merged output file. This should be provided if merging a
   sharded BAM file or any other BGZIPed format.

This command does not support Parquet output, so the only `default
options <#default-args>`__ that this command supports is
``-print_metrics``. Note ``-print_metrics`` is deprecated in ADAM version
0.31.0 and will be removed in version 0.32.0.

coverage
~~~~~~~~~~~~~~

The ``coverage`` command computes per-locus coverage from reads
and saves the coverage counts as features. Takes two required arguments:

1. ``INPUT``: The input path. A file containing alignments in any of the
   supported ADAM alignment input formats.
2. ``OUTPUT``: The path to save the coverage counts to. Saves in any of
   the ADAM supported feature file formats.

In addition to the `default options <#default-args>`__,
``coverage`` takes the following options:

-  ``-collapse``: If two (or more) neighboring sites have the same
   coverage, we collapse them down into a single genomic feature.
-  ``-only_negative_strands``: Only computes coverage for reads aligned
   on the negative strand. Conflicts with ``-only_positive_strands``.
-  ``-only_positive_strands``: Only computes coverage for reads aligned
   on the positive strand. Conflicts with ``-only_negative_strands``.
-  ``-sort_lexicographically``: Sorts coverage by position. Contigs are
   ordered lexicographically. Only applies if running with
   ``-collapse``.

