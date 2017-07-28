# Running ADAM's command line tools {#cli}

In addition to being used as an API for [building applications](#apps), ADAM
provides a command line interface (CLI) for extracting, transforming, and
loading (ETL-ing) genomics data. Our CLI is roughly divided into three sections:

* [Actions](#actions) that manipulate data using the ADAM schemas
* [Conversions](#conversions) that convert data from legacy formats into Parquet
* [Printers](#printers) that provide detailed or summarized views of genomic
  data

ADAM's various CLI actions can be run from the command line using the
`scripts/adam-submit` script. This script uses the `spark-submit` script to run
an ADAM application on a Spark cluster. To use this script, either
`spark-submit` must be on the `$PATH`, or the `$SPARK_HOME` environment variable
must be set.

#### Default arguments {#default-args}

There are several command line options that are present across most commands.
These include:

* `-h`, `-help`, `--help`, `-?`: prints the usage for this command
* `-parquet_block_size N`: sets the block size for Parquet in bytes, if writing
  a Parquet output file. Defaults to 128 MB (128 * 1024 * 1024).
* `-parquet_compression_codec`: The codec to use for compressing a Parquet page.
  Choices are:
    * `UNCOMPRESSED`: No compression.
    * `SNAPPY`: Use the [Snappy](https://github.com/google/snappy) compression
      codec.
    * `GZIP`: Use a [Gzip](https://www.gnu.org/software/gzip/) based compression
      codec.
    * `LZO`: Use a
      [LZO](https://en.wikipedia.org/wiki/Lempel%E2%80%93Ziv%E2%80%93Oberhumer)
      based compression codec. To use LZO, the [LZO libraries must be
      installed](http://hbase.apache.org/book.html#trouble.rs.startup.compression).
* `-parquet_disable_dictionary`: Disables dictionary encoding in Parquet, and
  enables delta encoding.
* `-parquet_logging_level VAL`: The [Log4j](http://logging.apache.org/log4j/)
  logging level to set for Parquet's loggers. Defaults to `severe`.
* `-parquet_page_size N`: The page size in bytes to use when writing Parquet
  files. Defaults to 1MB (1024 * 1024).
* `-print_metrics`: If provided, prints the
  [instrumentation](https://github.com/bigdatagenomics/utils#instrumentation)
  metrics to the log when the CLI operation terminates.

#### Legacy output options {#legacy-output}

Several tools in ADAM support saving back to legacy genomics output formats. Any
tool saving to one of these formats supports the following options:

* `-single`: Merge sharded output files. If this is not provided, the output
  will be written as sharded files where each shard is a valid file. If this
  _is_ provided, the shards will be written without headers as a
  `${OUTPUTNAME}_tail` directory, and a single header will be written to
  `${OUTPUTNAME}_head`. If `-single` is provided and `-defer_merging` is _not_
  provided, the header file and the shard directory will be merged into a single
  file at `${OUTPUTPATH}`.
* `-defer_merging`: If both `-defer_merging` and `-single` are provided, the
  output will be saved as if is a single file, but the output files will not be
  merged.
* `-disable_fast_concat`: If `-single` is provided and `-defer_merging` is not,
  this disables the use of the parallel concatenation engine. This engine is
  used when running on top of HDFS, and resizes the output files to match the
  HDFS block size before calling the Hadoop FileSystem `concat` method which
  concatenates the files by modifying the filesystem metadata and not the bytes
  on disk. This method is vastly more performant for large files, but has many
  invariants that may prevent it from being run (e.g., it cannot be run on an
  encrypted HDFS directory).

#### Validation stringency {#validation}

Various components in ADAM support passing a validation stringency level. This
is a three level scale:

* `STRICT`: If validation fails, throw an exception.
* `LENIENT`: If validation fails, ignore the data and write a warning to the
  log.
* `SILENT`: If validation fails, ignore the data silently.

## Action tools {#actions}

Roughly speaking, "action" tools apply some form of non-trivial transformation
to data using the ADAM APIs.

### countKmers and countContigKmers {#countKmers}

Counts the $k$ length substrings in either a set of reads or reference
fragments. Takes three required arguments:

1. `INPUT`: The input path. A set of reads for `countKmers` or a set of
  reference contigs for `countContigKmers`.
2. `OUTPUT`: The path to save the output to. Saves the output as
 [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) containing
 the $k$-mer sequence and count.
3. `KMER_LENGTH`: The length $k$ of substrings to count.

Beyond the [default options](#default-args), both `countKmers` and
`countContigKmers` take one option:

* `-print_histogram`: If provided, prints a histogram of the $k$-mer count
  distribution to standard out.

### transformAlignments {#transformAlignments}

The `transformAlignments` CLI is the entrypoint to ADAM's read preprocessing
tools. This command provides drop-in replacement commands for several commands
in the [Genome Analysis Toolkit](https://software.broadinstitute.org/gatk/)
"Best Practices" read preprocessing pipeline and more [@depristo11]. This CLI
tool takes two required arguments:

1. `INPUT`: The input path. A file containing reads in any of the supported
  ADAM read input formats.
2. `OUTPUT`: The path to save the transformed reads to. Supports any of ADAM's
  read output formats.

Beyond the [default options](#default-args) and the [legacy output
options](#legacy-output), `transformAlignments` supports a vast range of options.
These options fall into several general categories:

* General options:
    * `-cache`: If provided, the results of intermediate stages will be cached.
      This is necessary to avoid recomputation if running multiple
      transformations (e.g., Indel realignment, BQSR, etc) back to back.
    * `-storage_level`: Along with `-cache`, this can be used to set the Spark
      [persistance level](http://spark.apache.org/docs/latest/programming-guide.html#which-storage-level-to-choose)
      for cached data. If not provided, this defaults to `MEM_ONLY`.
    * `-stringency`: Sets the validation stringency for various operations.
      Defaults to `LENIENT.` See [validation stringency](#validation) for more
      details.
* Loading options:
    * `-repartition`: Forces a repartition on load. Useful to increase the
      available parallelism on a small dataset. Forces a shuffle. Takes the
      number of partitions to repartition to.
    * `-force_load_bam`: Forces ADAM to try to load the input as SAM/BAM/CRAM.
    * `-force_load_fastq`: Forces ADAM to try to load the input as FASTQ.
    * `-paired_fastq`: Forces `-force_load_fastq`, and passes the path of a
      second-of-pair FASTQ file to load.
    * `-record_group`: If loading FASTQ, sets the record group name on each
      read to this value.
    * `-force_load_ifastq`: Forces ADAM to try to load the input as interleaved
      FASTQ.
    * `-force_load_parquet`: Forces ADAM to try to load the input as Parquet
      encoded using the ADAM `AlignmentRecord` schema.
    * `-limit_projection`: If loading as Parquet, sets a projection that does
      not load the `attributes` or `origQual` fields of the `AlignmentRecord`.
    * `-aligned_read_predicate`: If loading as Parquet, only loads aligned
      reads.
    * `-region_predicate`: A string indicating that reads should be filtered on
      overlapping a genomic position or range. This argument takes a comma
      separated list, where each element in the list takes the form:
      * `contig:pos` for a single genomic position, or
      * `contig:start-end` for a genomic range with closed start and open end
      E.g., `-region_predicate 1:100,2:1000-2000` would filter all reads that
      overlapped either position 100 on `1` or the range from 1,000 to 2,000
      on `2`.
    * `-concat`: Provides a path to an optional second file to load, which is
      then concatenated to the file given as the `INPUT` path.
* Duplicate marking options: Duplicate marking is run with the
  `-mark_duplicate_reads` option. It takes no optional parameters.
* BQSR options: BQSR is run with the `-recalibrate_base_qualities` flag.
  Additionally, the BQSR engine takes the following parameters:
    * `-known_snps`: Path to a VCF file/Parquet variant file containing known
      point variants. These point variants are used to mask read errors during
      recalibration. Specifically, putative read errors that are at variant
      sites are treated as correct observations. If BQSR is run, this option
      should be passed, along with a path to a known variation database (e.g.,
      [dbSNP](https://www.ncbi.nlm.nih.gov/projects/SNP/)). {#known-snps}
* Indel realignment options: Indel realignment is run with the `-realign_indels`
  flag. Additionally, the Indel realignment engine takes the following options:
    * `-known_indels`: Path to a VCF file/Parquet variant file containing known
      Indel variants to realign against. If provided, forces the `KNOWNS_ONLY`
      consensus model. If not provided, forces the `CONSENSUS_FROM_READS` model.
      See [candidate generation and realignment](#consensus-model). {#known-indels}
    * `-max_consensus_number`: The maximum number of consensus sequences to
      realign a single target against. If more consensus sequences are seen at
      a single target, we randomly downsample. Defaults to 30.
    * `-max_indel_size`: The maximum length of an Indel to realign against.
      Indels longer than this size are dropped before generating consensus
      sequences. Defaults to 500bp.
    * `-max_target_size`: The maximum length of a target to realign. Targets
      longer than this size are dropped before trying to realign. Defaults to
      3,000bp.
    * `-max_reads_per_target`: The maximum number of reads in a target that
      we will try to realign. By default, this value is 20,000 reads. If we
      encounter a target with more than this number of reads, we skip realigning
      this target.
    * `-reference`: An optional path to a reference genome assembly build. If this
      is not provided, then we attempt to reconstruct the reference at each target
      from the MD tags on each read.
    * `-unclip_reads`: If provided, we will unclip reads when attempting to realign
      them to the reference. If not provided, we leave clipped reads clipped.
    * `-log_odds_threshold`: The log odds threshold to use for picking a
      consensus sequence to finalize realignments against. A consensus will not
      be realigned against unless the Phred weighted edit distance against the
      given consensus/reference pair is a sufficient improvement over the
      original reference realignments. This option sets that improvement weight.
      Defaults to 5.0.
* Base quality binning: If the `-bin_quality_scores` option is passed, the quality
  scores attached to the reads will be rewritten into bins. This option takes a
  semicolon (`;`) delimited list, where each element describes a bin. The
  description for a bin is three integers: the bottom of the bin, the top of the
  bin, and the value to assign to bases in the bin. E.g., given the description
  `0,20,10:20,50,30`, all quality scores between 0--19 will be rewritten to 10,
  and all quality scores between 20--49 will be rewritten to 30.
* `mismatchingPositions` tagging options: We can recompute the
  `mismatchingPositions` field of an AlignmentRecord (SAM "MD" tag) with the
  `-add_md_tags` flag. This flag takes a path to a reference file in either
  FASTA or Parquet `NucleotideContigFragment` format. Additionally, this engine
  takes the following options:
    * `-md_tag_fragment_size`: If loading from FASTA, sets the size of each
      fragment to load. Defaults to 10,000bp.
    * `-md_tag_overwrite`: If provided, recomputes and overwrites the
      `mismatchingPositions` field for records where this field was provided.
* Output options: `transformAlignments` supports the [legacy output](#legacy-output)
  options. Additionally, there are the following options:
    * `-coalesce`: Sets the number of partitions to coalesce the output to.
      If `-force_shuffle_coalesce` is not provided, the Spark engine may ignore
      the coalesce directive.
    * `-force_shuffle_coalesce`: Forces a shuffle that leads to the output being
      saved with the number of partitions requested by `-coalesce`. This is
      necessary if the `-coalesce` would increase the number of partitions, or
      if it would reduce the number of partitions to fewer than the number of
      Spark executors. This may have a substantial performance cost, and will
      invalidate any sort order.
    * `-sort_reads`: Sorts reads by alignment position. Unmapped reads are
      placed at the end of all reads. Contigs are ordered by sequence record
      index.
    * `-sort_lexicographically`: Sorts reads by alignment position. Unmapped
      reads are placed at the end of all reads. Contigs are ordered
      lexicographically.
    * `-sort_fastq_output`: Ignored if not saving to FASTQ. If saving to FASTQ,
      sorts the output reads by read name.

### transformFeatures

Loads a feature file into the ADAM `Feature` schema, and saves it back. The
input and output formats are autodetected. Takes two required arguments:

1. `INPUT`: The input path. A file containing features in any of the supported
  ADAM feature input formats.
2. `OUTPUT`: The path to save the transformed features to. Supports any of ADAM's
  feature output formats.

Beyond the [default options](#default-args) and the [legacy output
options]{#legacy-output}, `transformFeatures` has one optional argument:

* `-num_partitions`: If loading from a textual feature format (i.e., not
  Parquet), sets the number of partitions to load. If not provided, this is
  chosen by Spark.

### transformGenotypes

Loads a genotype file into the ADAM `Genotype` schema, and saves it back. The
input and output formats are autodetected. Takes two required arguments:

1. `INPUT`: The input path. A file containing genotypes in any of the supported
  ADAM genotype input formats.
2. `OUTPUT`: The path to save the transformed genotypes to. Supports any of ADAM's
  genotype output formats.

Beyond the [default options](#default-args) and the [legacy output
options]{#legacy-output}, `transformGenotypes` has additional arguments:

* `-coalesce`: Sets the number of partitions to coalesce the output to.
  If `-force_shuffle_coalesce` is not provided, the Spark engine may ignore
  the coalesce directive.
* `-force_shuffle_coalesce`: Forces a shuffle that leads to the output being
  saved with the number of partitions requested by `-coalesce`. This is
  necessary if the `-coalesce` would increase the number of partitions, or
  if it would reduce the number of partitions to fewer than the number of
  Spark executors. This may have a substantial performance cost, and will
  invalidate any sort order.
* `-sort_on_save`: Sorts the genotypes when saving, where contigs are ordered
  by sequence index. Conflicts with `-sort_lexicographically_on_save`.
* `-sort_lexicographically_on_save`: Sorts the genotypes when saving, where
  contigs are ordered lexicographically. Conflicts with `-sort_on_save`.
* `-single`: Saves the VCF file as headerless shards, and then merges the
  sharded files into a single VCF.
* `-stringency`: Sets the validation stringency for conversion.
  Defaults to `LENIENT.` See [validation stringency](#validation) for more
  details.

In this command, the validation stringency is applied to the
individual genotypes. If a genotype fails validation, the
individual genotype will be dropped (for lenient or silent validation,
under strict validation, conversion will fail). Header lines are not validated.
Due to a constraint imposed by the [htsjdk](https://github.com/samtools/htsjdk)
library, which we use to parse VCF files, user provided header lines that do not
match the header line definitions from the
[VCF 4.2](https://samtools.github.io/hts-specs/VCFv4.2.pdf) spec will be
overridden with the line definitions from the specification. Unfortunately, this
behavior cannot be disabled. If there is a user provided vs. spec mismatch in
format/info field count or type, this will likely cause validation failures
during conversion.

### transformVariants

Loads a variant file into the ADAM `Variant` schema, and saves it back. The
input and output formats are autodetected. Takes two required arguments:

1. `INPUT`: The input path. A file containing variants in any of the supported
  ADAM variant input formats.
2. `OUTPUT`: The path to save the transformed variants to. Supports any of ADAM's
  variant output formats.

Beyond the [default options](#default-args) and the [legacy output
options]{#legacy-output}, `transformVariants` has additional arguments:

* `-coalesce`: Sets the number of partitions to coalesce the output to.
  If `-force_shuffle_coalesce` is not provided, the Spark engine may ignore
  the coalesce directive.
* `-force_shuffle_coalesce`: Forces a shuffle that leads to the output being
  saved with the number of partitions requested by `-coalesce`. This is
  necessary if the `-coalesce` would increase the number of partitions, or
  if it would reduce the number of partitions to fewer than the number of
  Spark executors. This may have a substantial performance cost, and will
  invalidate any sort order.
* `-sort_on_save`: Sorts the variants when saving, where contigs are ordered
  by sequence index. Conflicts with `-sort_lexicographically_on_save`.
* `-sort_lexicographically_on_save`: Sorts the variants when saving, where
  contigs are ordered lexicographically. Conflicts with `-sort_on_save`.
* `-single`: Saves the VCF file as headerless shards, and then merges the
  sharded files into a single VCF.
* `-stringency`: Sets the validation stringency for conversion.
  Defaults to `LENIENT.` See [validation stringency](#validation) for more
  details.

In this command, the validation stringency is applied to the
individual variants. If a variant fails validation, the
individual variant will be dropped (for lenient or silent validation,
under strict validation, conversion will fail). Header lines are not validated.
Due to a constraint imposed by the [htsjdk](https://github.com/samtools/htsjdk)
library, which we use to parse VCF files, user provided header lines that do not
match the header line definitions from the
[VCF 4.2](https://samtools.github.io/hts-specs/VCFv4.2.pdf) spec will be
overridden with the line definitions from the specification. Unfortunately, this
behavior cannot be disabled. If there is a user provided vs. spec mismatch in
format/info field count or type, this will likely cause validation failures
during conversion.

### mergeShards

A CLI tool for merging a [sharded legacy file](#legacy-output) that was written
with the `-single` and `-defer_merging` flags. Runs the file merging process.
Takes two required arguments:

1. `INPUT`: The input directory of sharded files to merge.
2. `OUTPUT`: The path to save the merged file at.

This command takes several optional arguments:

* `-buffer_size`: The buffer size in bytes to use for copying data on the
  driver. Defaults to 4MB (4 * 1024 * 1024).
* `-header_path`: The path to a header file that should be written to the start
  of the merged output.
* `-write_cram_eof`: Writes an empty CRAM container at the end of the merged
  output file. This should not be provided unless merging a sharded CRAM file.
* `-write_empty_GZIP_at_eof`: Writes an empty GZIP block at the end of the
  merged output file. This should be provided if merging a sharded BAM file or
  any other BGZIPed format.

This command does not support Parquet output, so the only [default
options](#default-args) that this command supports is `-print_metrics`.

### reads2coverage

The `reads2coverage` command computes per-locus coverage from reads and saves
the coverage counts as features. Takes two required arguments:

1. `INPUT`: The input path. A file containing reads in any of the supported
  ADAM read input formats.
2. `OUTPUT`: The path to save the coverage counts to. Saves in any of the ADAM
  supported feature file formats.

In addition to the [default options](#default-args), `reads2coverage` takes the
following options:

* `-collapse`: If two (or more) neighboring sites have the same coverage, we
  collapse them down into a single genomic feature.
* `-only_negative_strands`: Only computes coverage for reads aligned on the
  negative strand. Conflicts with `-only_positive_strands`.
* `-only_positive_strands`: Only computes coverage for reads aligned on the
  positive strand. Conflicts with `-only_negative_strands`.
* `-sort_lexicographically`: Sorts coverage by position. Contigs are ordered
  lexicographically. Only applies if running with `-collapse`.

## Conversion tools {#conversions}

These tools convert data between a legacy genomic file format and using ADAM's
schemas to store data in Parquet.

### fasta2adam and adam2fasta

These commands convert between FASTA and Parquet files storing assemblies using
the NucleotideContigFragment schema.

`fasta2adam` takes two required arguments:

1. `FASTA`: The input FASTA file to convert.
2. `ADAM`: The path to save the Parquet formatted NucleotideContigFragments to.

`fasta2adam` supports the full set of [default options](#default-args), as well
as the following options:

* `-fragment_length`: The fragment length to shard a given contig into. Defaults
  to 10,000bp.
* `-reads`: Path to a set of reads that includes sequence info. This read path
  is used to obtain the sequence indices for ordering the contigs from the
  FASTA file.
* `-repartition`: The number of partitions to save the data to. If provided,
  forces a shuffle.
* `-verbose`: If given, enables additional logging where the sequence dictionary
  is printed.

`adam2fasta` takes two required arguments:

1. `ADAM`: The path to a Parquet file containing NucleotideContigFragments.
2. `FASTA`: The path to save the FASTA file to.

`adam2fasta` only supports the `-print_metrics` option from the [default
options](#default-args). Additionally, `adam2fasta` takes the following options:

* `-line_width`: The line width in characters to use for breaking FASTA lines.
  Defaults to 60 characters.
* `-coalesce`: Sets the number of partitions to coalesce the output to.
  If `-force_shuffle_coalesce` is not provided, the Spark engine may ignore
  the coalesce directive.
* `-force_shuffle_coalesce`: Forces a shuffle that leads to the output being
  saved with the number of partitions requested by `-coalesce`. This is
  necessary if the `-coalesce` would increase the number of partitions, or
  if it would reduce the number of partitions to fewer than the number of
  Spark executors. This may have a substantial performance cost, and will
  invalidate any sort order.

### adam2fastq

While the [`transformAlignments`](#transformAlignments) command can export to
FASTQ, the `adam2fastq` provides a simpler CLI with more output options.
`adam2fastq` takes two required arguments and an optional third argument:

1. `INPUT`: The input read file, in any ADAM-supported read format.
2. `OUTPUT`: The path to save an unpaired or interleaved FASTQ file to, or the
  path to save the first-of-pair reads to, for paired FASTQ.
3. Optional `SECOND_OUTPUT`: If saving paired FASTQ, the path to save the
  second-of-pair reads to.

`adam2fastq` only supports the `-print_metrics` option from the [default
options](#default-args). Additionally, `adam2fastq` takes the following options:

* `-no_projection`: By default, `adam2fastq` only projects the fields necessary
  for saving to FASTQ. This option disables that projection and projects all
  fields.
* `-output_oq`: Outputs the original read qualities, if available.
* `-persist_level`: Sets the Spark
  [persistence level](http://spark.apache.org/docs/latest/programming-guide.html#which-storage-level-to-choose)
  for cached data during the conversion back to FASTQ. If not provided, the
  intermediate RDDs are not cached.
* `-repartition`: The number of partitions to save the data to. If provided,
  forces a shuffle.
* `-validation`: Sets the validation stringency for checking whether reads are
  paired when saving paired reads. Defaults to `LENIENT.` See [validation
  stringency](#validation) for more details.

### transformFragments

These two commands translate read data between the single read alignment and
fragment representations.

`transformFragments` takes two required arguments:

1. `INPUT`: The input fragment file, in any ADAM-supported read or fragment format.
2. `OUTPUT`: The path to save reads at, in any ADAM-supported read or fragment format.

`transformFragments` takes the [default options](#default-args). Additionally,
`transformFragments` takes the following options:

* `-mark_duplicate_reads`: Marks reads as fragment duplicates. Running mark
  duplicates on fragments improves performance by eliminating one `groupBy`
  (and therefore, a shuffle) versus running on reads.
* Base quality binning: If the `-bin_quality_scores` option is passed, the quality
  scores attached to the reads will be rewritten into bins. This option takes a
  semicolon (`;`) delimited list, where each element describes a bin. The
  description for a bin is three integers: the bottom of the bin, the top of the
  bin, and the value to assign to bases in the bin. E.g., given the description
  `0,20,10:20,50,30`, all quality scores between 0--19 will be rewritten to 10,
  and all quality scores between 20--49 will be rewritten to 30.
* `-load_as_reads`: Treats the input as a read file (uses `loadAlignments`
  instead of `loadFragments`), which behaves differently for unpaired FASTQ.
* `-save_as_reads`: Saves the output as a Parquet file of `AlignmentRecord`s,
  as SAM/BAM/CRAM, or as FASTQ, depending on the output file extension.
  If this option is specified, the output can also be sorted:
  * `-sort_reads`: Sorts reads by alignment position. Unmapped reads are
    placed at the end of all reads. Contigs are ordered by sequence record
    index.
  * `-sort_lexicographically`: Sorts reads by alignment position. Unmapped
    reads are placed at the end of all reads. Contigs are ordered
    lexicographically.

## Printing tools {#printers}

The printing tools provide some form of user readable view of an ADAM file.
These commands are useful for both quality control and debugging.

### print

Dumps a Parquet file to either the console or a text file as
[JSON](http://www.json.org). Takes one required argument:

1. `FILE(S)`: The file paths to load. These must be Parquet formatted files.

This command has several options:

* `-pretty`: Pretty print's the JSON output.
* `-o`: Provides a path to save the output dump to, instead of writing the
  output to the console.

This command does not support Parquet output, so the only [default
options](#default-args) that this command supports is `-print_metrics`.

### flagstat {#flagstat}

Runs the ADAM equivalent to the
[SAMTools](http://www.htslib.org/doc/samtools.html) `flagstat` command. Takes
one required argument:

1. `INPUT`: The input path. A file containing reads in any of the supported
  ADAM read input formats.

This command has several options:

* `-stringency`: Sets the validation stringency for various operations.
  Defaults to `SILENT.` See [validation stringency](#validation) for more
  details.
* `-o`: Provides a path to save the output dump to, instead of writing the
  output to the console.

This command does not support Parquet output, so the only [default
options](#default-args) that this command supports is `-print_metrics`.

### view

Runs the ADAM equivalent to the 
[SAMTools](http://www.htslib.org/doc/samtools.html) `view` command. Takes
one required argument:

1. `INPUT`: The input path. A file containing reads in any of the supported
  ADAM read input formats.

In addition to the [default options](#default-args), this command supports the
following options:

* `-o`: Provides a path to save the output dump to, instead of writing the
  output to the console. Format is autodetected as any of the ADAM read outputs.
* `-F`/`-f`: Filters reads that either match all (`-f`) or none (`-F`) of the
  flag bits.
* `-G`/`-g`: Filters reads that either mismatch all (`-g`) or none (`-G`) of the
  flag bits.
* `-c`: Prints the number of reads that (mis)matched the filters, instead of the
  reads themselves. Conflicts with `-o`.
