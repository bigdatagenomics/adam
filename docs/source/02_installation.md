# Building ADAM from Source

You will need to have [Maven](http://maven.apache.org/) installed in order to build ADAM.

> **Note:** The default configuration is for Hadoop 2.6.0. If building against a different
> version of Hadoop, please edit the build configuration in the `<properties>` section of
> the `pom.xml` file.

```bash
$ git clone https://github.com/bigdatagenomics/adam.git
$ cd adam
$ export "MAVEN_OPTS=-Xmx512m -XX:MaxPermSize=128m"
$ mvn clean package -DskipTests
```
Outputs
```
...
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 9.647s
[INFO] Finished at: Thu May 23 15:50:42 PDT 2013
[INFO] Final Memory: 19M/81M
[INFO] ------------------------------------------------------------------------
```

You might want to take a peek at the `scripts/jenkins-test` script and give it a run. It will fetch a mouse chromosome, encode it to ADAM
reads and pileups, run flagstat, etc. We use this script to test that ADAM is working correctly.

## Running ADAM

ADAM is packaged via [appassembler](http://mojo.codehaus.org/appassembler/appassembler-maven-plugin/) and includes all necessary
dependencies

You might want to add the following to your `.bashrc` to make running ADAM easier:

```bash
alias adam-submit="${ADAM_HOME}/bin/adam-submit"
alias adam-shell="${ADAM_HOME}/bin/adam-shell"
```

`$ADAM_HOME` should be the path to where you have checked ADAM out on your local filesystem. 
The first alias should be used for running ADAM jobs that operate locally. The latter two aliases 
call scripts that wrap the `spark-submit` and `spark-shell` commands to set up ADAM. You'll need
to have the Spark binaries on your system; prebuilt binaries can be downloaded from the
[Spark website](http://spark.apache.org/downloads.html). Currently, our continuous builds default to
[Spark 1.5.2 built against Hadoop 2.6 (CDH5)](http://d3kbcqa49mib13.cloudfront.net/spark-1.5.2-bin-hadoop2.6.tgz),
but any more recent Spark distribution should also work.

Once this alias is in place, you can run ADAM by simply typing `adam-submit` at the commandline, e.g.

```bash
$ adam-submit
```
Outputs:

       e         888~-_          e             e    e
      d8b        888   \        d8b           d8b  d8b
     /Y88b       888    |      /Y88b         d888bdY88b
    /  Y88b      888    |     /  Y88b       / Y88Y Y888b
   /____Y88b     888   /     /____Y88b     /   YY   Y888b
  /      Y88b    888_-~     /      Y88b   /          Y888b

Usage: adam-submit [<spark-args> --] <adam-args>

Choose one of the following commands:

ADAM ACTIONS
               depth : Calculate the depth from a given ADAM file, at each variant in a VCF
         count_kmers : Counts the k-mers/q-mers from a read dataset.
  count_contig_kmers : Counts the k-mers/q-mers from a read dataset.
           transform : Convert SAM/BAM to ADAM format and optionally perform read pre-processing transformations
          adam2fastq : Convert BAM to FASTQ files
              plugin : Executes an ADAMPlugin
             flatten : Convert a ADAM format file to a version with a flattened schema, suitable for querying with tools like Impala

CONVERSION OPERATIONS
            vcf2adam : Convert a VCF file to the corresponding ADAM format
           anno2adam : Convert a annotation file (in VCF format) to the corresponding ADAM format
            adam2vcf : Convert an ADAM variant to the VCF ADAM format
          fasta2adam : Converts a text FASTA sequence file into an ADAMNucleotideContig Parquet file which represents assembled sequences.
          adam2fasta : Convert ADAM nucleotide contig fragments to FASTA files
       features2adam : Convert a file with sequence features into corresponding ADAM format
          wigfix2bed : Locally convert a wigFix file to BED format
     fragments2reads : Convert alignment records into fragment records.
     reads2fragments : Convert alignment records into fragment records.

PRINT
               print : Print an ADAM formatted file
         print_genes : Load a GTF file containing gene annotations and print the corresponding gene models
            flagstat : Print statistics on reads in an ADAM file (similar to samtools flagstat)
          print_tags : Prints the values and counts of all tags in a set of records
            listdict : Print the contents of an ADAM sequence dictionary
         allelecount : Calculate Allele frequencies
                view : View certain reads from an alignment-record file.
```

ADAM outputs all the commands that are available for you to run. To get
help for a specific command, run `adam-submit <command>` without any additional arguments.

```bash
$ adam-submit transform --help
```
Outputs:
```
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
