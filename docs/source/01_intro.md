# Introduction

ADAM is a genomics analysis platform with specialized file formats built using [Apache Avro](http://avro.apache.org), [Apache Spark](http://spark.apache.org/) and [Parquet](http://parquet.io/). Apache 2 licensed.

* [Follow](https://twitter.com/bigdatagenomics/) our Twitter account
* [Chat](https://gitter.im/bigdatagenomics/adam) with ADAM developers on Gitter
* [Join](http://bdgenomics.org/mail) our mailing list
* [Check out](https://amplab.cs.berkeley.edu/jenkins/view/Big%20Data%20Genomics/) the current build status
* [Download](https://github.com/bigdatagenomics/adam/releases) official releases
* [View](http://search.maven.org/#search%7Cga%7C1%7Corg.bdgenomics) our software artifacts on Maven Central
* [See](https://oss.sonatype.org/index.html#nexus-search;quick~bdgenomics) our snapshots
* [Look](https://github.com/bigdatagenomics/adam/blob/master/CHANGES.md) at our CHANGES file  

## Apache Spark

[Apache Spark](http://spark.apache.org/) allows developers to write algorithms in succinct code that can run fast locally, on an in-house cluster or on Amazon, Google or Microsoft clouds. 

For example, the following code snippet will print the top ten 21-mers in `NA2114` from 1000 Genomes:

```scala
val ac = new ADAMContext(sc)
// Load alignments from disk
val reads = ac.loadAlignments(
  "/data/NA21144.chrom11.ILLUMINA.adam",
  predicate = Some(classOf[ExamplePredicate]),
  projection = Some(Projection(
    AlignmentRecordField.sequence,
    AlignmentRecordField.readMapped,
    AlignmentRecordField.mapq)))
// Generate, count and sort 21-mers
val kmers = reads.flatMap { read =>
  read.getSequence.sliding(21).map(k => (k, 1L))
}.reduceByKey((k1: Long, k2: Long) => k1 + k2)
  .map(_.swap)
  .sortByKey(ascending = false)
// Print the top 10 most common 21-mers
kmers.take(10).foreach(println)
```

Executing this Spark job will output the following:

```
(121771,TTTTTTTTTTTTTTTTTTTTT)
(44317,ACACACACACACACACACACA)
(44023,TGTGTGTGTGTGTGTGTGTGT)
(42474,CACACACACACACACACACAC)
(42095,GTGTGTGTGTGTGTGTGTGTG)
(33797,TAATCCCAGCACTTTGGGAGG)
(33081,AATCCCAGCACTTTGGGAGGC)
(32775,TGTAATCCCAGCACTTTGGGA)
(32484,CCTCCCAAAGTGCTGGGATTA)
```

You do not need to be a Scala developer to use ADAM. You could also run the following ADAM CLI command for the same result:

```bash
adam-submit count_kmers \
       /data/NA21144.chrom11.ILLUMINA.adam \
       /data/results.txt 21
```

## Apache Parquet

[Apache Parquet](http://parquet.apache.org) is a columnar storage format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model or programming language.

- Parquet compresses legacy genomic formats using standard columnar techniques (e.g. RLE, dictionary encoding). ADAM files are typically ~20% smaller than compressed BAM files.
- Parquet integrates with:
    - **Query engines**: Hive, Impala, HAWQ, IBM Big SQL, Drill, Tajo, Pig, Presto
    - **Frameworks**: Spark, MapReduce, Cascading, Crunch, Scalding, Kite
    - **Data models**: Avro, Thrift, ProtocolBuffers, POJOs
- Parquet is simply a file format which makes it easy to sync and share data using tools like `distcp`, `rsync`, etc
- Parquet provides a command-line tool, `parquet.hadoop.PrintFooter`, which reports useful compression statistics 

In the counting k-mers example above, you can see that there is a defined *predicate* and *projection*. The *predicate* allows rapid filtering of rows while a *projection* allows you to efficiently materialize only specific columns for analysis. For this k-mer counting example, we filter out any records that are not mapped or have a `MAPQ` less than 20 using a `predicate` and only materialize the `Sequence`, `ReadMapped` flag and `MAPQ` columns and skip over all other fields like `Reference` or `Start` position, e.g.

Sequence| ReadMapped | MAPQ | ~~Reference~~ | ~~Start~~ | ...
--------|------------|------|-----------|-------|-------
~~GGTCCAT~~ | ~~false~~ | - | ~~chrom1~~ | - | ...
TACTGAA | true | 30 | ~~chrom1~~ | ~~34232~~ | ...
~~TTGAATG~~ | ~~true~~ | ~~17~~ | ~~chrom1~~ | ~~309403~~ | ...

## Apache Avro

- Apache Avro is a data serialization system ([http://avro.apache.org](http://avro.apache.org))
- All Big Data Genomics schemas are published at [https://github.com/bigdatagenomics/bdg-formats](https://github.com/bigdatagenomics/bdg-formats)
- Having explicit schemas and self-describing data makes integrating, sharing and evolving formats easier

Our Avro schemas are directly converted into source code using Avro tools. Avro supports a number of computer languages. ADAM uses Java; you could 
just as easily use this Avro IDL description as the basis for a Python project. Avro currently supports C, C++, C#, Java, JavaScript, PHP, Python and Ruby. 

## More than k-mer counting

ADAM does much more than just k-mer counting. Running the ADAM CLI without arguments or with `--help` will display available commands.

```bash
$ adam-submit

       e         888~-_          e             e    e
      d8b        888   \        d8b           d8b  d8b
     /Y88b       888    |      /Y88b         d888bdY88b
    /  Y88b      888    |     /  Y88b       / Y88Y Y888b
   /____Y88b     888   /     /____Y88b     /   YY   Y888b
  /      Y88b    888_-~     /      Y88b   /          Y888b

Usage: adam-submit [<spark-args> --] <adam-args>

Choose one of the following commands:

ADAM ACTIONS
          countKmers : Counts the k-mers/q-mers from a read dataset.
    countContigKmers : Counts the k-mers/q-mers from a read dataset.
           transform : Convert SAM/BAM to ADAM format and optionally perform read pre-processing transformations
   transformFeatures : Convert a file with sequence features into corresponding ADAM format and vice versa
  transformGenotypes : Convert a file with genotypes into corresponding ADAM format and vice versa
   transformVariants : Convert a file with variants into corresponding ADAM format and vice versa
         mergeShards : Merges the shards of a file
      reads2coverage : Calculate the coverage from a given ADAM file

CONVERSION OPERATIONS
          fasta2adam : Converts a text FASTA sequence file into an ADAMNucleotideContig Parquet file which represents assembled sequences.
          adam2fasta : Convert ADAM nucleotide contig fragments to FASTA files
          adam2fastq : Convert BAM to FASTQ files
     fragments2reads : Convert alignment records into fragment records.
     reads2fragments : Convert alignment records into fragment records.

PRINT
               print : Print an ADAM formatted file
            flagstat : Print statistics on reads in an ADAM file (similar to samtools flagstat)
                view : View certain reads from an alignment-record file.
```

You can learn more about a command, by calling it without arguments or with `--help`.

```bash
$ adam-submit transformAlignments
Argument "INPUT" is required
 INPUT                                                           : The ADAM, BAM or SAM file to apply the transforms to
 OUTPUT                                                          : Location to write the transformed data in ADAM/Parquet format
 -add_md_tags VAL                                                : Add MD Tags to reads based on the FASTA (or equivalent) file passed to this option.
 -aligned_read_predicate                                         : Only load aligned reads. Only works for Parquet files.
 -cache                                                          : Cache data to avoid recomputing between stages.
 -coalesce N                                                     : Set the number of partitions written to the ADAM output directory
 -concat VAL                                                     : Concatenate this file with <INPUT> and write the result to <OUTPUT>
 -dump_observations VAL                                          : Local path to dump BQSR observations to. Outputs CSV format.
 -force_load_bam                                                 : Forces Transform to load from BAM/SAM.
 -force_load_fastq                                               : Forces Transform to load from unpaired FASTQ.
 -force_load_ifastq                                              : Forces Transform to load from interleaved FASTQ.
 -force_load_parquet                                             : Forces Transform to load from Parquet.
 -force_shuffle_coalesce                                         : Even if the repartitioned RDD has fewer partitions, force a shuffle.
 -h (-help, --help, -?)                                          : Print help
 -known_indels VAL                                               : VCF file including locations of known INDELs. If none is provided, default
                                                                   consensus model will be used.
 -known_snps VAL                                                 : Sites-only VCF giving location of known SNPs
 -limit_projection                                               : Only project necessary fields. Only works for Parquet files.
 -log_odds_threshold N                                           : The log-odds threshold for accepting a realignment. Default value is 5.0.
 -mark_duplicate_reads                                           : Mark duplicate reads
 -max_consensus_number N                                         : The maximum number of consensus to try realigning a target region to. Default
                                                                   value is 30.
 -max_indel_size N                                               : The maximum length of an INDEL to realign to. Default value is 500.
 -max_target_size N                                              : The maximum length of a target region to attempt realigning. Default length is
                                                                   3000.
 -md_tag_fragment_size N                                         : When adding MD tags to reads, load the reference in fragments of this size.
 -md_tag_overwrite                                               : When adding MD tags to reads, overwrite existing incorrect tags.
 -paired_fastq VAL                                               : When converting two (paired) FASTQ files to ADAM, pass the path to the second file
                                                                   here.
 -parquet_block_size N                                           : Parquet block size (default = 128mb)
 -parquet_compression_codec [UNCOMPRESSED | SNAPPY | GZIP | LZO] : Parquet compression codec
 -parquet_disable_dictionary                                     : Disable dictionary encoding
 -parquet_logging_level VAL                                      : Parquet logging level (default = severe)
 -parquet_page_size N                                            : Parquet page size (default = 1mb)
 -print_metrics                                                  : Print metrics to the log on completion
 -realign_indels                                                 : Locally realign indels present in reads.
 -recalibrate_base_qualities                                     : Recalibrate the base quality scores (ILLUMINA only)
 -record_group VAL                                               : Set converted FASTQs' record-group names to this value; if empty-string is passed,
                                                                   use the basename of the input file, minus the extension.
 -repartition N                                                  : Set the number of partitions to map data to
 -single                                                         : Saves OUTPUT as single file
 -sort_fastq_output                                              : Sets whether to sort the FASTQ output, if saving as FASTQ. False by default.
                                                                   Ignored if not saving as FASTQ.
 -sort_reads                                                     : Sort the reads by referenceId and read position
 -storage_level VAL                                              : Set the storage level to use for caching.
 -stringency VAL                                                 : Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults
                                                                   to LENIENT
```

The ADAM transformAlignments command allows you to mark duplicates, run base quality score recalibration (BQSR) and other pre-processing steps on your data.

There are also a number of projects built on ADAM:

- [Avocado](https://github.com/bigdatagenomics/avocado) is a variant caller built on top of ADAM for germline and somatic calling
- [Mango](https://github.com/bigdatagenomics/mango) is a library for visualizing large scale genomics data with interactive latencies
