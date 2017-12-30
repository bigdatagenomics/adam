# ADAM

ADAM is a library and command line tool that enables the use of [Apache
Spark](https://spark.apache.org) to parallelize genomic data analysis across
cluster/cloud computing environments. ADAM uses a set of schemas to describe
genomic sequences, reads, variants/genotypes, and features, and can be used
with data in legacy genomic file formats such as SAM/BAM/CRAM, BED/GFF3/GTF,
and VCF, as well as data stored in the columnar
[Apache Parquet](https://parquet.apache.org) format. On a single node, ADAM
provides competitive performance to optimized multi-threaded tools, while
enabling scale out to clusters with more than a thousand cores. ADAM's APIs
can be used from Scala, Java, Python, R, and SQL.

## Documentation

ADAM's documentation is hosted at [readthedocs](http://adam.readthedocs.io).

## Python Requirements

ADAM depends on having PySpark installed.