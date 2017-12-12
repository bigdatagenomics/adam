Deploying ADAM
==============

Running ADAM on AWS EC2 using CGCloud
-------------------------------------

CGCloud provides an automated means to create a cluster on EC2 for use
with ADAM.

| `CGCloud <https://github.com/BD2KGenomics/cgcloud>`__ lets you
  automate the creation, management and provisioning of VMs and clusters
  of VMs in Amazon EC2.
| The `CGCloud plugin for
  Spark <https://github.com/BD2KGenomics/cgcloud/blob/master/spark/README.rst>`__
  lets you setup a fully configured Apache Spark cluster in EC2.

Prior to following these instructions, make sure you have set up your
AWS account and know your AWS access keys. See https://aws.amazon.com/
for details.

Configure CGCloud
~~~~~~~~~~~~~~~~~

Begin by reading the CGcloud
`readme <https://github.com/BD2KGenomics/cgcloud>`__.

Next, configure `CGCloud
core <https://github.com/BD2KGenomics/cgcloud/blob/master/core/README.rst>`__
and then install the `CGcloud spark
plugin <https://github.com/BD2KGenomics/cgcloud/blob/master/spark/README.rst>`__.

| One modification to CGCloud install instructions: replace the two pip
  calls
| ``pip install cgcloud-core`` and ``pip install cgcloud-spark`` with
  the single command:

::

    pip install cgcloud-spark==1.6.0

which will install the correct version of both cgcloud-core and
cgcloud-spark.

Note, the steps to register your ssh key and create the template boxes
only need to be done once.

::

    cgcloud register-key ~/.ssh/id_rsa.pub
    cgcloud create generic-ubuntu-trusty-box
    cgcloud create -IT spark-box

Launch a cluster
~~~~~~~~~~~~~~~~

Spin up a Spark cluster named ``cluster1`` with one leader and two
worker nodes of instance type ``m3.large`` with the command:

::

    cgcloud create-cluster spark -c cluster1 -s 2 -t m3.large

Once running, you can ssh to ``spark-master`` with the command:

::

    cgcloud ssh -c cluster1 spark-master

Spark is already installed on the ``spark-master`` machine and slaves,
test it by starting a spark-shell.

::

    spark-shell
    exit()

Install ADAM
~~~~~~~~~~~~

To use the ADAM application on top of Spark, we need to download and
install ADAM on ``spark-master``. From the command line on
``spark-master`` download a release
`here <https://github.com/bigdatagenomics/adam/releases>`__. As of this
writing, CGCloud supports Spark 1.6.2, not Spark 2.x, so download the
Spark 1.x Scala2.10 release:

::

    wget https://repo1.maven.org/maven2/org/bdgenomics/adam/adam-distribution_2.10/0.20.0/adam-distribution_2.10-0.20.0-bin.tar.gz
    tar -xvfz adam-distribution_2.10-0.20.0-bin.tar.gz

You can now run ``./bin/adam-submit`` and ``./bin/adam-shell`` using
your EC2 cluster.

Input and Output data on HDFS and S3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Spark requires a file system, such as HDFS or a network file mount, that
all machines can access. The CGCloud EC2 Spark cluster you just created
is already running HDFS.

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

Terminate Cluster
~~~~~~~~~~~~~~~~~

Shutdown the cluster using:

::

    cgcloud terminate-cluster -c cluster1 spark

CGCloud options and Spot Instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

View help docs for all options of the ``cgcloud create-cluster``
command:

::

    cgcloud create-cluster -h

In particular, note the ``--spot-bid`` and related spot options to
utilize AWS spot instances in order to save on costs. To avoid
unintended costs, it is a good idea to use the AWS console to double
check that your instances have terminated.

Accessing the Spark GUI
~~~~~~~~~~~~~~~~~~~~~~~

In order to view the Spark server or application GUI pages on port 4040
and 8080 on ``spark-master``, go to Security Groups in the AWS console
and open inbound TCP for those ports from your local IP address. Find
the IP address of ``spark-master``, which is part of the Linux command
prompt. On your local machine, you can then open
``http://ip_of_spark_master:4040/`` in a web browser, where
``ip_of_spark_master`` is replaced with the IP address you found.
