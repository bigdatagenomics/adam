Running ADAM's command line tools
=================================

In addition to being used as an API for `building
applications <#apps>`__, ADAM provides a command line interface (CLI)
for extracting, transforming, and loading (ETL-ing) genomics data. Our
CLI is roughly divided into three sections:

-  `Actions <#actions>`__ that manipulate data using the ADAM schemas
-  `Conversions <#conversions>`__ that convert data from legacy formats
   into Parquet
-  `Printers <#printers>`__ that provide detailed or summarized views of
   genomic data

ADAM's various CLI actions can be run from the command line using the
``scripts/adam-submit`` script. This script uses the ``spark-submit``
script to run an ADAM application on a Spark cluster. To use this
script, either ``spark-submit`` must be on the ``$PATH``, or the
``$SPARK_HOME`` environment variable must be set.

Default arguments
-----------------

There are several command line options that are present across most
commands. These include:

-  ``-h``, ``-help``, ``--help``, ``-?``: prints the usage for this
   command
-  ``-parquet_block_size N``: sets the block size for Parquet in bytes,
   if writing a Parquet output file. Defaults to 128 MB (128 \* 1024 \*
   1024).
-  ``-parquet_compression_codec``: The codec to use for compressing a
   Parquet page. Choices are:

   -  ``UNCOMPRESSED``: No compression.
   -  ``SNAPPY``: Use the `Snappy <https://github.com/google/snappy>`__
      compression codec.
   -  ``GZIP``: Use a `Gzip <https://www.gnu.org/software/gzip/>`__
      based compression codec.
   -  ``LZO``: Use a
      `LZO <https://en.wikipedia.org/wiki/Lempel%E2%80%93Ziv%E2%80%93Oberhumer>`__
      based compression codec. To use LZO, the `LZO libraries must be
      installed <http://hbase.apache.org/book.html#trouble.rs.startup.compression>`__.

-  ``-parquet_disable_dictionary``: Disables dictionary encoding in
   Parquet, and enables delta encoding.
-  ``-parquet_logging_level VAL``: The
   `Log4j <http://logging.apache.org/log4j/>`__ logging level to set for
   Parquet's loggers. Defaults to ``severe``.
-  ``-parquet_page_size N``: The page size in bytes to use when writing
   Parquet files. Defaults to 1MB (1024 \* 1024).
-  ``-print_metrics``: If provided, prints the
   `instrumentation <https://github.com/bigdatagenomics/utils#instrumentation>`__
   metrics to the log when the CLI operation terminates.

Legacy output options
---------------------

Several tools in ADAM support saving back to legacy genomics output
formats. Any tool saving to one of these formats supports the following
options:

-  ``-single``: Merge sharded output files. If this is not provided, the
   output will be written as sharded files where each shard is a valid
   file. If this *is* provided, the shards will be written without
   headers as a ``${OUTPUTNAME}_tail`` directory, and a single header
   will be written to ``${OUTPUTNAME}_head``. If ``-single`` is provided
   and ``-defer_merging`` is *not* provided, the header file and the
   shard directory will be merged into a single file at
   ``${OUTPUTPATH}``.
-  ``-defer_merging``: If both ``-defer_merging`` and ``-single`` are
   provided, the output will be saved as if is a single file, but the
   output files will not be merged.
-  ``-disable_fast_concat``: If ``-single`` is provided and
   ``-defer_merging`` is not, this disables the use of the parallel
   concatenation engine. This engine is used when running on top of
   HDFS, and resizes the output files to match the HDFS block size
   before calling the Hadoop FileSystem ``concat`` method which
   concatenates the files by modifying the filesystem metadata and not
   the bytes on disk. This method is vastly more performant for large
   files, but has many invariants that may prevent it from being run
   (e.g., it cannot be run on an encrypted HDFS directory).

Validation stringency
---------------------

Various components in ADAM support passing a validation stringency
level. This is a three level scale:

-  ``STRICT``: If validation fails, throw an exception.
-  ``LENIENT``: If validation fails, ignore the data and write a warning
   to the log.
-  ``SILENT``: If validation fails, ignore the data silently.

Partitioned output
------------------

Various commands in ADAM support saving partitioned Parquet output. These
commands take the following parameters, which control the output:

-  ``-partition_by_start_pos``: If true, enables saving a partitioned dataset.
-  ``-partition_bin_size``: The size of each partition bin in base pairs.
   Defaults to 1Mbp (1,000,000 base pairs).
