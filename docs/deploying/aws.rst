Deploying ADAM on AWS
=====================

Input and Output data on HDFS and S3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Apache Spark requires a file system, such as HDFS or a network file mount,
that all machines can access.

The typical flow of data to and from your ADAM application on EC2 will
be:

-  Upload data to AWS S3
-  Transfer from S3 to the HDFS on your cluster
-  Compute with ADAM, write output to HDFS
-  Copy data you wish to persist for later use to S3

For small test files you may wish to skip S3 by uploading directly to
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

Bulk Transfer between HDFS and S3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To transfer large amounts of data back and forth from S3 to HDFS, we
suggest using `Conductor <https://github.com/BD2KGenomics/conductor>`__.
It is also possible to directly use AWS S3 as a distributed file system,
but with some loss of performance.

Conductor currently does not support uploading Apache Avro records in
Parquet directories to S3, as are written out by ADAM.  For uploads from
HDFS to S3, we suggest using `s3-dist-cp <https://docs.aws.amazon.com/emr/latest/ReleaseGuide/UsingEMR_s3distcp.html>`__.


Directly accessing data stored in S3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To directly access data stored in S3, we can leverage one of the Hadoop
`FileSystem API
implementations <https://wiki.apache.org/hadoop/AmazonS3>`__ that access
S3. Specifically, we recommend using the ``S3a`` file system. To do
this, you will need to configure your Spark job to use S3a. If you are
using a vendor-supported Spark distribution like `Amazon
EMR <https://aws.amazon.com/emr/>`__ or
`Databricks <https://databricks.com/>`__, your Spark distribution may
already have the S3a file system installed. If not, you will need to add
JARs that contain the classes needed to support the S3a file system. For
most Spark distributions built for Apache Hadoop 2.6 or higher, you will
need to add the following dependencies:

-  `com.amazonaws:aws-java-sdk-pom:1.10.34 <http://search.maven.org/#artifactdetails%7Ccom.amazonaws%7Caws-java-sdk-pom%7C1.10.34%7Cjar>`__
-  `org.apache.hadoop:hadoop-aws:2.7.4 <http://search.maven.org/#artifactdetails%7Corg.apache.hadoop%7Chadoop-aws%7C2.7.4%7Cjar>`__

Instead of downloading these JARs, you can ask Spark to install them at
runtime using the ``--packages`` flag. Additionally, if you are using
the S3a file system, your file paths will need to begin with the
``s3a://`` scheme:

::

    adam-submit \
      --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.4 \
      -- \
      transformAlignments \
      s3a://my_bucket/my_reads.adam \
      s3a://my_bucket/my_new_reads.adam

If you are loading a BAM, CRAM, or VCF file, you will need to add an
additional JAR. This is because the code that loads data stored in these
file formats uses Java's ``nio`` package to read index files. Java's
``nio`` system allows users to specify a "file system provider," which
implements ``nio``\ 's file system operations on non-POSIX file systems
like `HDFS <https://github.com/damiencarol/jsr203-hadoop>`__ or S3. To
use these file formats with the ``s3a://`` scheme, you should include
the following dependency:

-  `net.fnothaft:jsr203-s3a:0.0.1 <http://search.maven.org/#artifactdetails%7Cnet.fnothaft%7Cjsr203-s3a%7C0.0.1%7Cjar>`__

You will need to do this even if you are not using the index for said
format.
