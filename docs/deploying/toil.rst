Running ADAM on Toil
--------------------

`Toil <https://github.com/BD2KGenomics/toil>`__ is a workflow management
tool that supports running multi-tool workflows. Unlike traditional
workflow managers that are limited to supporting jobs that run on a
single node, Toil includes support for clusters of long lived services
through the Service Job abstraction. This abstraction enables workflows
that mix Spark-based tools like ADAM in with traditional, single-node
tools. (Vivian et al. 2016) describes the Toil architecture and
demonstrates the use of Toil at scale in the Amazon Web Services EC2
cloud. Toil can be run on various on-premises High Performance Computing
schedulers, and on the Amazon EC2 and Microsoft Azure clouds. A quick
start guide to deploying Toil in the cloud or in an on-premises cluster
can be found `here <https://toil.readthedocs.io>`__.

`toil-lib <https://github.com/BD2KGenomics/toil-lib>`__ is a library
downstream from Toil that provides common functionality that is useful
across varied genomics workflows. There are two useful modules that help
to set up an Apache Spark cluster, and to run an ADAM job:

-  ``toil_lib.spark``: This module contains all the code necessary to
   set up a set of Service Jobs that launch and run an Apache Spark
   cluster backed by the Apache Hadoop Distributed File System (HDFS).
-  ``toil_lib.tools.spark_tools``: This module contains functions that
   run ADAM in Toil using `Docker <https://www.docker.com>`__, as well
   as `Conductor <https://github.com/BD2KGenomics/conductor>`__, a tool
   for running transfers between HDFS and `Amazon's
   S3 <https://aws.amazon.com/s3>`__ storage service.

Several example workflows that run ADAM in Toil can be found in
`toil-scripts <https://github.com/BD2KGenomics/toil-scripts>`__. These
workflows include:

-  `adam-kmers <https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/adam_kmers>`__:
   this workflow was demonstrated in (Vivian et al. 2016) and sets up a
   Spark cluster which then runs ADAM's `countKmers CLI <#countKmers>`__.
-  `adam-pipeline <https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/adam_pipeline>`__:
   this workflow runs several stages in the ADAM
   `transformAlignments CLI <#transformAlignments>`__. This pipeline
   is the ADAM equivalent to the GATK's "Best Practice" read
   preprocessing pipeline. We then stitch together this pipeline with
   `BWA-MEM <https://github.com/lh3/bwa>`__ and the GATK in the
   `adam-gatk-pipeline <https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/adam_gatk_pipeline>`__.

An example workflow: count_kmers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For an example of how to use ADAM with Toil, let us look at the
`toil\_scripts.adam\_kmers.count\_kmers <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py>`__
module. This module has three parts:

-  `A main
   method <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L177-L228>`__
   that configures and launches a Toil workflow.
-  `A job
   function <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L22-L76>`__
   that launches both the Spark cluster service and the ADAM job.
-  `A child job
   function <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L78-L174>`__
   that calls ADAM and
   `Conductor <https://github.com/BD2KGenomics/conductor>`__ to transfer
   a BAM file from S3, convert that BAM file to Parquet, count *k*-mers,
   and upload the *k*-mer counts back to S3.

Configuring and launching Toil
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Toil takes most of its configuration from the command line. To make this
easy, Toil includes a function in the ``toil.job.Job`` class to register
Toil's argument parsing code with the Python standard
`argparse <https://docs.python.org/2/library/argparse.html>`__
library. E.g., in
`count_kmers.py <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L183-L214>`__,
we set up our arguments and then add the Toil specific arguments by:

::

        parser = argparse.ArgumentParser()

        # add parser arguments
        parser.add_argument('--input_path',
                            help='The full path to the input SAM/BAM/ADAM/FASTQ file.')
        parser.add_argument('--output-path',
                            help='full path where final results will be output.')
        parser.add_argument('--kmer-length',
                            help='Length to use for k-mer counting. Defaults to 20.',
                            default=20,
                            type=int)
        parser.add_argument('--spark-conf',
                            help='Optional configuration to pass to Spark commands. Either this or --workers must be specified.',
                            default=None)
        parser.add_argument('--memory',
                            help='Optional memory configuration for Spark workers/driver. This must be specified if --workers is specified.',
                            default=None,
                            type=int)
        parser.add_argument('--cores',
                            help='Optional core configuration for Spark workers/driver. This must be specified if --workers is specified.',
                            default=None,
                            type=int)
        parser.add_argument('--workers',
                            help='Number of workers to spin up in Toil. Either this or --spark-conf must be specified. If this is specified, --memory and --cores must be specified.',
                            default=None,
                            type=int)
        parser.add_argument('--sudo',
                            help='Run docker containers with sudo. Defaults to False.',
                            default=False,
                            action='store_true')

        Job.Runner.addToilOptions(parser)

Then, `we parse the arguments and start
Toil <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L215-L225>`__:

::

        args = parser.parse_args()
        Job.Runner.startToil(Job.wrapJobFn(kmer_dag,
                                           args.kmer_length,
                                           args.input_path,
                                           args.output_path,
                                           args.spark_conf,
                                           args.workers,
                                           args.cores,
                                           args.memory,
                                           args.sudo,
                                           checkpoint=True), args)

Note that we are passing the parsed arguments to the
``Job.Runner.startToil`` function. The other argument that we are
passing is the
`Job <https://toil.readthedocs.io/en/latest/developing.html#job-basics>`__
that we would like Toil to run. In this example, Toil is wrapping the
``kmer_dag`` function (discussed in the next section) up as a Job. The
``Job.wrapJobFn`` call takes the ``kmer_dag`` function and all of the
arguments that are being passed and serializes them so they can be run
locally or on a remote node. Additionally, we pass the optional argument
``checkpoint=True``. This argument indicates that the ``kmer_dag`` Job
function is a "checkpoint" job. If a job is a checkpoint job and any of
its children jobs fail, then we are saying that the workflow can be
successfully rerun from this point. In Toil, service jobs should always
be launched from a checkpointed job in order to allow the service jobs
to successfully resume after a service job failure.

More detailed information about launching a Toil workflow can be found
in the `Toil
documentation <https://toil.readthedocs.io/en/latest/developing.html#invoking-a-workflow>`__.

Launching a Spark Service
^^^^^^^^^^^^^^^^^^^^^^^^^

In the ``toil_scripts.adam_kmers.count_kmers`` example, we wrap the
``kmer_dag`` function as a job, and then use this function to launch a
Spark cluster as a set of service jobs using the ``toil_lib.spark``
module. Once we've done that, we also launch a job to run ADAM by
starting the ``download_count_upload`` child job function. `We launch
the Spark service
cluster <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L66-L69>`__
by calling the ``spawn_spark_cluster``\ function, which was imported
from the ``toil_lib.spark`` module:

::

            master_hostname = spawn_spark_cluster(job,
                                                  workers,
                                                  cores)

This function takes in three parameters:

-  ``job``: A handle to the currently running Toil Job. This is used to
   enqueue the service jobs needed to start the Spark cluster.
-  ``workers``: The number of Spark workers to allocate.
-  ``cores``: The number of cores to request per worker/leader node.

When called, this method does not return a hostname string. Rather, it
returns a
`promise <https://toil.readthedocs.io/en/latest/developing.html#promises>`__
for the hostname string. This promise is not valid inside of the
``kmer_dag`` job, but will be valid in the child job
(``download_count_upload``) that runs Spark. Toil cannot guarantee that
the Spark Service job will start until after the job that enqueues it
completes.

Finally, `we enqueue the child job that runs ADAM and
Conductor <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L73-L76>`__:

::

        job.addChildJobFn(download_count_upload,
                          masterHostname,
                          input_file, output_file, kmer_length,
                          spark_conf, memory, sudo)

Detailed documentation for the ``toil_lib.spark`` module can be found in
the `toil-lib
docs <https://github.com/BD2KGenomics/toil-lib/tree/master/docs>`__.

Running ADAM and other Spark applications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once we have enqueued the Spark service jobs and the child job that
interacts with the services, we can launch Spark applications from the
child job. In our example application, our `child job
function <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L78-L174>`__
does the following work:

1. `We check to see if the input file is already in
   HDFS <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L113-L117>`__:

::

        if master_ip is not None:
            hdfs_dir = "hdfs://{0}:{1}/".format(master_ip, HDFS_MASTER_PORT)
        else:
            _log.warn('Master IP is not set. If default filesystem is not set, jobs may fail.')
            hdfs_dir = ""

2. `If it is not in HDFS, we copy it in using
   Conductor <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L119-L129>`__:

::

        # if the file is not already in hdfs, copy it in
        hdfs_input_file = hdfs_dir
        if input_file.startswith("s3://"):

            # append the s3 file name to our hdfs path
            hdfs_input_file += input_file.split("/")[-1]

            # run the download
            _log.info("Downloading input file %s to %s.", input_file, hdfs_input_file)
            call_conductor(master_ip, input_file, hdfs_input_file,
                           memory=memory, override_parameters=spark_conf)

3. `We check to see if the file is a Parquet file, and convert it to
   Parquet if it is
   not <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L143-L159>`__:

::

        # do we need to convert to adam?
        if (hdfs_input_file.endswith('.bam') or
            hdfs_input_file.endswith('.sam') or
            hdfs_input_file.endswith('.fq') or
            hdfs_input_file.endswith('.fastq')):
            
            hdfs_tmp_file = hdfs_input_file

            # change the file extension to adam
            hdfs_input_file = '.'.join(hdfs_input_file.split('.')[:-1].append('adam'))

            # convert the file
            _log.info('Converting %s into ADAM format at %s.', hdfs_tmp_file, hdfs_input_file)
            call_adam(master_ip,
                      ['transformAlignments',
                       hdfs_tmp_file, hdfs_input_file],
                      memory=memory, override_parameters=spark_conf)

4. `We use the ADAM CLI to count the k-mers in the
   file <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L161-L168>`__:

::

        # run k-mer counting
        _log.info('Counting %d-mers in %s, and saving to %s.',
                  kmer_length, hdfs_input_file, hdfs_output_file)
        call_adam(master_ip,
                  ['countKmers',
                   hdfs_input_file, hdfs_output_file,
                   str(kmer_length)],
                  memory=memory, override_parameters=spark_conf)

5. `If requested, we use Conductor to copy the *k*-mer counts back to
   S3 <https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L170-L174>`__:

::

        # do we need to upload the file back? if so, run upload
        if run_upload:
            _log.info("Uploading output file %s to %s.", hdfs_output_file, output_file)
            call_conductor(master_ip, hdfs_output_file, output_file,
                           memory=memory, override_parameters=spark_conf)

The ``call_adam`` and ``call_conductor`` functions are imported from the
``toil_lib.tools.spark_tools`` module. These functions run ADAM and
Conductor using Docker containers from
`cgl-docker-lib <https://github.com/BD2KGenomics/cgl-docker-lib>`__. [1]_
These two functions launch the Docker containers using the
``call_docker`` function from the ``toil_lib.programs`` module, and do
some basic configuration of the command line. In the ADAM example, all
the user needs to pass is the exact arguments that they would like run
from the ADAM CLI, and the Spark configuration parameters that are
passed to the ``adam-submit`` script are automatically configured.

As you may have noticed, all of this functionality is contained in a
single Toil job. This is important for fault tolerance. Toil provides
tolerance against data loss through the use of a `file
store <https://toil.readthedocs.io/en/latest/developing.html#managing-files-within-a-workflow>`__,
which manages the persistance of local files to a persistant store
(e.g., S3). Since we store intermediate files in HDFS, thus bypassing
the file store, our intermediate results are not persistant, and thus
individual Spark applications are not atomic.

Using PySpark in Toil
~~~~~~~~~~~~~~~~~~~~~

As an aside, a nice benefit of Toil is that we can run PySpark jobs
inline with Toil workflows. A small demo of this is seen in the
``toil_lib.spark`` `unit
tests <https://github.com/BD2KGenomics/toil-lib/blob/master/src/toil_lib/test/test_spark.py#L58-L71>`__:

::

    def _count_child(job, masterHostname):

        # noinspection PyUnresolvedReferences
        from pyspark import SparkContext

        # start spark context and connect to cluster
        sc = SparkContext(master='spark://%s:7077' % masterHostname,
                          appName='count_test')

        # create an rdd containing 0-9999 split across 10 partitions
        rdd = sc.parallelize(xrange(10000), 10)
        
        # and now, count it
        assert rdd.count() == 10000

.. [1]
   These containers are published on
   `Quay <https://quay.io/repository/ucsc_cgl>`__.

