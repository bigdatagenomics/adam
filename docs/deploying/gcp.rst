Deploying ADAM on GCP
=====================

Input and Output data on HDFS and Google Cloud Storage (GCS)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Apache Spark requires a file system, such as HDFS or a network file mount,
that all machines can access.

The typical flow of data to and from your ADAM application on GCE will
be:

-  Upload data to GCS
-  Transfer from GCS to the HDFS on your cluster
-  Compute with ADAM, write output to HDFS
-  Copy data you wish to persist for later use to GCS

For small test files you may wish to skip GCS by uploading directly to
spark-master using ``scp`` and then copying to HDFS using:

::

    hadoop fs -put sample1.bam /datadir/

From the ADAM shell, or as a parameter to ADAM submit, you would refer
to HDFS URLs like this:

::

    adam-submit \
      transformAlignments \
      hdfs://spark-master/work_dir/sample1.bam \
      hdfs://spark-master/work_dir/sample1.adam

Directly accessing data stored in GCS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To directly access data stored in GCS, we recommend using 
`Cloud Dataproc <https://cloud.google.com/dataproc/>`__ 
so that your Spark distribution will already have the `GCS connector <https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage>`__ installed. 

::

    adam-submit \
      transformAlignments \
      gs://my_bucket/my_reads.adam \
      gs://my_bucket/my_new_reads.adam

If you are loading a BAM, CRAM, or VCF file, you will need to add an
additional JAR. This is because the code that loads data stored in these
file formats uses Java's ``nio`` package to read index files. Java's
``nio`` system allows users to specify a "file system provider," which
implements ``nio``\ 's file system operations on non-POSIX file systems
like `HDFS <https://github.com/damiencarol/jsr203-hadoop>`__ or GCS. To
use these file formats with the ``gs://`` scheme, you should include
the `Google Cloud nio dependency <http://search.maven.org/#artifactdetails%7Ccom.google.cloud%7Cgoogle-cloud-nio%7C0.22.0-alpha%7Cjar>`__.

Conveniently, it is possible to pass this into Spark as a prebuilt shaded jar using the --jars flag.

The prebuilt jar can be found at `https://oss.sonatype.org/content/repositories/releases/com/google/cloud/google-cloud-nio/0.22.0-alpha/google-cloud-nio-0.22.0-alpha-shaded.jar <https://oss.sonatype.org/content/repositories/releases/com/google/cloud/google-cloud-nio/0.22.0-alpha/google-cloud-nio-0.22.0-alpha-shaded.jar>`__

::
  
    adam-submit \
      transformAlignments \
      gs://my_bucket/my_reads.adam \
      gs://my_bucket/my_new_reads.adam \
      --jars google-cloud-nio-0.22.0-alpha-shaded.jar



You will need to do this even if you are not using the index for said
format.
