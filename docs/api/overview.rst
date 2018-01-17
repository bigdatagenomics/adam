API Overview
============

The main entrypoint to ADAM is the `ADAMContext <#adam-context>`__,
which allows genomic data to be loaded in to Spark as
`GenomicRDD <#genomic-rdd>`__. GenomicRDDs can be transformed using
ADAM's built in `pre-processing algorithms <#algorithms>`__, `Spark's
RDD primitives <#transforming>`__, the `region join <#join>`__
primitive, and ADAM's `pipe <#pipes>`__ APIs. GenomicRDDs can also be
interacted with as `Spark SQL tables <#sql>`__.

In addition to the Scala/Java API, ADAM can be used from
`Python <#python>`__ and `R <#r>`__.

Adding dependencies on ADAM libraries
-------------------------------------

ADAM libraries are available from `Maven
Central <http://search.maven.org>`__ under the groupId
``org.bdgenomics.adam``, such as the ``adam-core`` library:

::

    <dependency>
      <groupId>org.bdgenomics.adam</groupId>
      <artifactId>adam-core-spark2_2.11</artifactId>
      <version>${adam.version}</version>
    </dependency>

Scala apps should depend on ``adam-core``, while Java applications
should also depend on ``adam-apis``:

::

    <dependency>
      <groupId>org.bdgenomics.adam</groupId>
      <artifactId>adam-apis-spark2_2.11</artifactId>
      <version>${adam.version}</version>
    </dependency>

Additionally, we push nightly SNAPSHOT releases of ADAM to the `Sonatype
snapshot
repo <https://oss.sonatype.org/content/repositories/snapshots/org/bdgenomics/adam/>`__,
for developers who are interested in working on top of the latest
changes in ADAM.

The ADAM Python API
-------------------

ADAM's Python API wraps the `ADAMContext <#adam-context>`__ and
`GenomicRDD <#genomic-rdd>`__ APIs so they can be used from PySpark. The
Python API is feature complete relative to ADAM's Java API, with the
exception of the `region join <#join>`__ API, which is not supported.

The ADAM R API
--------------

ADAM's R API wraps the `ADAMContext <#adam-context>`__ and
`GenomicRDD <#genomic-rdd>`__ APIs so they can be used from SparkR. The
R API is feature complete relative to ADAM's Java API, with the
exception of the `region join <#join>`__ API, which is not supported.

