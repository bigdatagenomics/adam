# Deploying ADAM

## Running ADAM on AWS EC2 using CGCloud

CGCloud provides an automated means to create a cluster on EC2 for use with ADAM.

[CGCloud](https://github.com/BD2KGenomics/cgcloud) lets you automate the creation, 
management and provisioning of VMs and clusters of VMs in Amazon EC2.  
The [CGCloud plugin for Spark](https://github.com/BD2KGenomics/cgcloud/blob/master/spark/README.rst) 
lets you setup a fully configured Apache Spark cluster in EC2.

Prior to following these instructions, make sure you have set up your AWS 
account and know your
AWS access keys.  See https://aws.amazon.com/ for details.

#### Configure CGCloud

Begin by reading the CGcloud [readme](https://github.com/BD2KGenomics/cgcloud).

Next, configure [CGCloud core](https://github.com/BD2KGenomics/cgcloud/blob/master/core/README.rst) 
and then install the 
[CGcloud spark plugin](https://github.com/BD2KGenomics/cgcloud/blob/master/spark/README.rst).

One modification to CGCloud install instructions: replace the two pip calls  
`pip install cgcloud-core` and `pip install cgcloud-spark` with the single command:
```
pip install cgcloud-spark==1.6.0
```
which will install the correct version of both cgcloud-core and cgcloud-spark.

Note, the steps to register your ssh key and create the template boxes only need to be done once.
```
cgcloud register-key ~/.ssh/id_rsa.pub
cgcloud create generic-ubuntu-trusty-box
cgcloud create -IT spark-box
```

#### Launch a cluster

Spin up a Spark cluster named `cluster1` with one leader and two worker nodes 
of instance type `m3.large` with the command:
```
cgcloud create-cluster spark -c cluster1 -s 2 -t m3.large
```
Once running, you can ssh to `spark-master` with the command:
```
cgcloud ssh -c cluster1 spark-master
```

Spark is already installed on the `spark-master` machine and slaves, test it 
by starting a spark-shell.
```
spark-shell
exit()
```

#### Install ADAM

To use the ADAM application on top of Spark, we need to download and install 
ADAM on `spark-master`.
From the command line on `spark-master` download a release
[here](https://github.com/bigdatagenomics/adam/releases). As of this writing, 
CGCloud supports Spark 1.6.2, not Spark 2.x, so download
the Spark 1.x Scala2.10 release:
```
wget https://repo1.maven.org/maven2/org/bdgenomics/adam/adam-distribution_2.10/0.20.0/adam-distribution_2.10-0.20.0-bin.tar.gz
tar -xvfz adam-distribution_2.10-0.20.0-bin.tar.gz
```

You can now run `./bin/adam-submit` and `./bin/adam-shell` using your EC2 
cluster.

#### Input and Output data on HDFS and S3

Spark requires a file system, such as HDFS or a network file mount, that all 
machines can access.
The CGCloud EC2 Spark cluster you just created is already running HDFS.

The typical flow of data to and from your ADAM application on EC2 will be:
- Upload data to AWS S3
- Transfer from S3 to the HDFS on your cluster
- Compute with ADAM, write output to HDFS
- Copy data you wish to persist for later use to S3

For small test files you may wish to skip S3 by uploading directly to 
spark-master using `scp` and then copying to HDFS using:
```
hadoop fs -put sample1.bam /datadir/
```

From the ADAM shell, or as a parameter to ADAM submit, you would refer to HDFS URLs like this:
```
adam-submit transformAlignments hdfs://spark-master/work_dir/sample1.bam \
                      hdfs://spark-master/work_dir/sample1.adam
```

#### Bulk Transfer between HDFS and S3

To transfer large amounts of data back and forth from S3 to HDFS, we suggest using 
[Conductor](https://github.com/BD2KGenomics/conductor).
It is also possible to directly use AWS S3 as a distributed file system, 
but with some loss of performance.

#### Terminate Cluster

Shutdown the cluster using:
```
cgcloud terminate-cluster -c cluster1 spark
```

#### CGCoud options and Spot Instances

View help docs for all options of the `cgcloud create-cluster` command:
```
cgcloud create-cluster -h
```

In particular, note the `--spot-bid` and related spot options to utilize AWS 
spot instances in order to save on costs. To avoid unintended costs, 
it is a good idea to use the AWS console to double check that your 
instances have terminated.

#### Accessing the Spark GUI

In order to view the Spark server or application GUI pages on port 4040 and 
8080 on `spark-master`, go to Security Groups in the AWS console
and open inbound TCP for those ports from your local IP address. Find the 
IP address of `spark-master`, which is part of the Linux command prompt. On your local machine, 
you can then open http://ip_of_spark_master:4040/ in a web browser, where ip_of_spark_master
is replaced with the IP address you found.

## Running ADAM on CDH 5, HDP, and other YARN based Distros

[Apache Hadoop YARN](http://hadoop.apache.org/docs/stable2/hadoop-yarn/hadoop-yarn-site/YARN.html)
is a widely used scheduler in the Hadoop ecosystem. YARN stands for "Yet Another
Resource Negotiator", and the YARN architecture is described in [@vavilapalli13].
YARN is used in several common Hadoop distributions, including the [Cloudera Hadoop
Distribution (CDH)](http://www.cloudera.com/products/apache-hadoop/key-cdh-components.html)
and the [Hortonworks Data Platform (HDP)](http://hortonworks.com/products/data-center/hdp/).
YARN is supported natively in [Spark](http://spark.apache.org/docs/latest/running-on-yarn.html).

The ADAM CLI and shell can both be run on YARN. The ADAM CLI can be run in both Spark's
YARN `cluster` and `client` modes, while the ADAM shell can only be run in `client` mode.
In the `cluster` mode, the Spark driver runs in the YARN `ApplicationMaster` container. In
the `client` mode, the Spark driver runs in the submitting process. Since the Spark driver
for the Spark/ADAM shell takes input on stdin, it cannot run in `cluster` mode.

To run the ADAM CLI in YARN `cluster` mode, run the following command:

```
./bin/adam-submit \
  --master yarn \
  --deploy-mode cluster \
  -- \
  <adam_command_name> [options] \
```

In the `adam-submit` command, all options before the `--` are passed to the
`spark-submit` script, which launches the Spark job. To run in `client` mode,
we simply change the `deploy-mode` to `client`:

```
./bin/adam-submit \
  --master yarn \
  --deploy-mode client \
  -- \
  <adam_command_name> [options] \
```

In the `adam-shell` command, all of the arguments are passed to the 
`spark-shell` command. Thus, to run the `adam-shell` on YARN, we run:

```
./bin/adam-shell \
  --master yarn \
  --deploy-mode client
```

All of these commands assume that the Spark assembly you are using is
properly configured for your YARN deployment. Typically, if your Spark
assembly is properly configured to use YARN, there will be a symbolic link at
`${SPARK_HOME}/conf/yarn-conf/` that points to the core Hadoop/YARN
configuration. Though, this may vary by the distribution you are running.

The full list of configuration options for running Spark-on-YARN can be found
[online](http://spark.apache.org/docs/latest/running-on-yarn.html#configuration).
Most of the standard configurations are consistent between Spark Standalone and
Spark-on-YARN. One important configuration option to be aware of is the
YARN memory overhead parameter. From 1.5.0 onwards, Spark makes aggressive
use of off-heap memory allocation in the JVM. These allocations may cause
the amount of memory taken up by a single executor (or, theoretically, the
driver) to exceed the `--driver-memory`/`--executor-memory` parameters. These
parameters are what Spark provides as a memory resource request to YARN. By
default, if one of your Spark containers (an executors or the driver) exceeds
its memory request, YARN will kill the container by sending a `SIGTERM`. This
can cause jobs to fail. To eliminate this issue, you can set the
`spark.yarn.<role>.memoryOverhead` parameter, where `<role>` is one of
`driver` or `executor`. This parameter is used by Spark to increase its
resource request to YARN over the JVM Heap size indicated by `--driver-memory`
or `--executor-memory`.

As a final example, to run the ADAM [transformAlignments](#transformAlignments)
CLI using YARN cluster mode on a 64 node cluster with one executor per node and
a 2GB per executor overhead, we would run:

```
./bin/adam-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 200g \
  --executor-memory 200g \
  --conf spark.driver.cores=16 \
  --conf spark.executor.cores=16 \
  --conf spark.yarn.executor.memoryOverhead=2048 \
  --conf spark.executor.instances=64 \
  -- \
  transformAlignments in.sam out.adam
```

In this example, we are allocating 200GB of JVM heap space per executor and for
the driver, and we are telling Spark to request 16 cores per executor and for
the driver.

## Running ADAM on Toil

[Toil](https://github.com/BD2KGenomics/toil) is a workflow management tool that
supports running multi-tool workflows. Unlike traditional workflow managers that
are limited to supporting jobs that run on a single node, Toil includes support
for clusters of long lived services through the Service Job abstraction. This
abstraction enables workflows that mix Spark-based tools like ADAM in with
traditional, single-node tools. [@vivian16] describes the Toil architecture
and demonstrates the use of Toil at scale in the Amazon Web Services EC2 cloud.
Toil can be run on various on-premises High Performance Computing schedulers,
and on the Amazon EC2 and Microsoft Azure clouds. A quick start guide to
deploying Toil in the cloud or in an on-premises cluster can be found [here](https://toil.readthedocs.io).

[toil-lib](https://github.com/BD2KGenomics/toil-lib) is a library downstream
from Toil that provides common functionality that is useful across varied
genomics workflows. There are two useful modules that help to set up an Apache
Spark cluster, and to run an ADAM job:

* `toil_lib.spark`: This module contains all the code necessary to set up a
  set of Service Jobs that launch and run an Apache Spark cluster backed by the
  Apache Hadoop Distributed File System (HDFS).
* `toil_lib.tools.spark_tools`: This module contains functions that run ADAM
  in Toil using [Docker](https://www.docker.com), as well as
  [Conductor](https://github.com/BD2KGenomics/conductor), a tool for running
  transfers between HDFS and [Amazon's S3](https://aws.amazon.com/s3) storage
  service.

Several example workflows that run ADAM in Toil can be found in
[toil-scripts](https://github.com/BD2KGenomics/toil-scripts). These workflows
include:

* [adam-kmers](https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/adam_kmers):
  this workflow was demonstrated in [@vivian16] and sets up a Spark cluster
  which then runs ADAM's [`countKmers` CLI](#countKmers).
* [adam-pipeline](https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/adam_pipeline):
  this workflow runs several stages in the ADAM [`transformAlignments` CLI](#transformAlignments).
  This pipeline is the ADAM equivalent to the GATK's "Best Practice" read
  preprocessing pipeline. We then stitch together this pipeline with
  [BWA-MEM](https://github.com/lh3/bwa) and the GATK in the [adam-gatk-pipeline](
  https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/adam_gatk_pipeline).

### An example workflow: `toil_scripts.adam_kmers.count_kmers`

For an example of how to use ADAM with Toil, let us look at the
[toil_scripts.adam_kmers.count_kmers](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py)
module. This module has three parts:

* [A main method](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L177-L228)
  that configures and launches a Toil workflow.
* [A job function](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L22-L76)
  that launches both the Spark cluster service and the ADAM job.
* [A child job function](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L78-L174)
  that calls ADAM and [Conductor](https://github.com/BD2KGenomics/conductor) to
  transfer a BAM file from S3, convert that BAM file to Parquet, count _k_-mers,
  and upload the _k_-mer counts back to S3.

#### Configuring and launching Toil

Toil takes most of its configuration from the command line. To make this easy,
Toil includes a function in the `toil.job.Job` class to register Toil's argument
parsing code with the [Python standard `argparse`](https://docs.python.org/2/library/argparse.html)
library. E.g., [in `count_kmers.py`](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L183-L214),
we set up our arguments and then add the Toil specific arguments by:

```
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
```

Then, [we parse the arguments and start Toil](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L215-L225):

```
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
```

Note that we are passing the parsed arguments to the `Job.Runner.startToil`
function. The other argument that we are passing is the [Job](https://toil.readthedocs.io/en/latest/developing.html#job-basics)
that we would like Toil to run. In this example, Toil is wrapping the `kmer_dag`
function (discussed in the next section) up as a Job. The `Job.wrapJobFn`
call takes the `kmer_dag` function and all of the arguments that are being
passed and serializes them so they can be run locally or on a remote node.
Additionally, we pass the optional argument `checkpoint=True`. This argument
indicates that the `kmer_dag` Job function is a "checkpoint" job. If a job is
a checkpoint job and any of its children jobs fail, then we are saying that
the workflow can be successfully rerun from this point. In Toil, service jobs
should always be launched from a checkpointed job in order to allow the
service jobs to successfully resume after a service job failure.

More detailed information about launching a Toil workflow can be found in the
[Toil documentation](https://toil.readthedocs.io/en/latest/developing.html#invoking-a-workflow).

#### Launching a Spark Service

In the `toil_scripts.adam_kmers.count_kmers` example, we wrap the `kmer_dag`
function as a job, and then use this function to launch a Spark cluster as a set
of service jobs using the `toil_lib.spark` module. Once we've done that, we also
launch a job to run ADAM by starting the `download_count_upload` child job
function. [We launch the Spark service cluster](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L66-L69)
by calling the `spawn_spark_cluster`function, which was imported from the
`toil_lib.spark` module:

```
        master_hostname = spawn_spark_cluster(job,
                                              workers,
                                              cores)
```

This function takes in three parameters:

* `job`: A handle to the currently running Toil Job. This is used to enqueue the
  service jobs needed to start the Spark cluster.
* `workers`: The number of Spark workers to allocate.
* `cores`: The number of cores to request per worker/leader node.

When called, this method does not return a hostname string. Rather, it returns a
[promise](https://toil.readthedocs.io/en/latest/developing.html#promises) for
the hostname string. This promise is not valid inside of the `kmer_dag` job, but
will be valid in the child job (`download_count_upload`) that runs Spark. Toil
cannot guarantee that the Spark Service job will start until after the job that
enqueues it completes.

Finally, [we enqueue the child job that runs ADAM and Conductor](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L73-L76):

```
    job.addChildJobFn(download_count_upload,
                      masterHostname,
                      input_file, output_file, kmer_length,
                      spark_conf, memory, sudo)
```

Detailed documentation for the `toil_lib.spark` module can be found in the
[toil-lib docs](https://github.com/BD2KGenomics/toil-lib/tree/master/docs).

#### Running ADAM and other Spark applications

Once we have enqueued the Spark service jobs and the child job that interacts with
the services, we can launch Spark applications from the child job. In our
example application, our [child job function](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L78-L174)
does the following work:

1. [We check to see if the input file is already in HDFS](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L113-L117):

```
    if master_ip is not None:
        hdfs_dir = "hdfs://{0}:{1}/".format(master_ip, HDFS_MASTER_PORT)
    else:
        _log.warn('Master IP is not set. If default filesystem is not set, jobs may fail.')
        hdfs_dir = ""
```

2. [If it is not in HDFS, we copy it in using Conductor](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L119-L129):

```
    # if the file is not already in hdfs, copy it in
    hdfs_input_file = hdfs_dir
    if input_file.startswith("s3://"):

        # append the s3 file name to our hdfs path
        hdfs_input_file += input_file.split("/")[-1]

        # run the download
        _log.info("Downloading input file %s to %s.", input_file, hdfs_input_file)
        call_conductor(master_ip, input_file, hdfs_input_file,
                       memory=memory, override_parameters=spark_conf)
```

3. [We check to see if the file is a Parquet file, and convert it to Parquet if it is not](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L143-L159):

```
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
```

4. [We use the ADAM CLI to count the _k_-mers in the file](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L161-L168):

```
    # run k-mer counting
    _log.info('Counting %d-mers in %s, and saving to %s.',
              kmer_length, hdfs_input_file, hdfs_output_file)
    call_adam(master_ip,
              ['countKmers',
               hdfs_input_file, hdfs_output_file,
               str(kmer_length)],
              memory=memory, override_parameters=spark_conf)
```

5. [If requested, we use Conductor to copy the _k_-mer counts back to S3](https://github.com/BD2KGenomics/toil-scripts/blob/master/src/toil_scripts/adam_kmers/count_kmers.py#L170-L174):

```
    # do we need to upload the file back? if so, run upload
    if run_upload:
        _log.info("Uploading output file %s to %s.", hdfs_output_file, output_file)
        call_conductor(master_ip, hdfs_output_file, output_file,
                       memory=memory, override_parameters=spark_conf)
```

The `call_adam` and `call_conductor` functions are imported from the
`toil_lib.tools.spark_tools` module. These functions run ADAM and Conductor
using Docker containers from [cgl-docker-lib](https://github.com/BD2KGenomics/cgl-docker-lib).[^dl]
These two functions launch the Docker containers using the `call_docker`
function from the `toil_lib.programs` module, and do some basic configuration of
the command line. In the ADAM example, all the user needs to pass is the exact
arguments that they would like run from the ADAM CLI, and the Spark
configuration parameters that are passed to the `adam-submit` script are
automatically configured.

[^dl]: These containers are published on [Quay](https://quay.io/repository/ucsc_cgl).

As you may have noticed, all of this functionality is contained in a single Toil
job. This is important for fault tolerance. Toil provides tolerance against data
loss through the use of a [file store](https://toil.readthedocs.io/en/latest/developing.html#managing-files-within-a-workflow),
which manages the persistance of local files to a persistant store (e.g., S3).
Since we store intermediate files in HDFS, thus bypassing the file store, our
intermediate results are not persistant, and thus individual Spark applications
are not atomic.

### Using PySpark in Toil

As an aside, a nice benefit of Toil is that we can run PySpark jobs inline with
Toil workflows. A small demo of this is seen in the `toil_lib.spark`
[unit tests](https://github.com/BD2KGenomics/toil-lib/blob/master/src/toil_lib/test/test_spark.py#L58-L71):

```
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
```

## Running ADAM on Slurm

For those groups with access to a HPC cluster with [Slurm](https://en.wikipedia.org/wiki/Slurm_Workload_Manager) 
managing a number of compute nodes with local and/or network attached storage, it is possible to spin up a 
temporary Spark cluster for use by ADAM.

The full IO bandwidth benefits of Spark processing are likely best realized through a set of 
co-located compute/storage nodes. However, depending on your network setup, you may find Spark deployed on HPC 
to be a workable solution for testing or even production at scale, especially for those applications 
which perform multiple in-memory transformations and thus benefit from Spark's in-memory processing model.

Follow the primary [instructions](https://github.com/bigdatagenomics/adam/blob/master/docs/source/02_installation.md) 
for installing ADAM into `$ADAM_HOME`.  This will most likely be at a location on a shared disk accessible to all nodes, but could be at a consistant location on each machine.

### Start Spark cluster

A Spark cluster can be started as a multi-node job in Slurm by creating a job file `run.cmd` such as below:
```bash
#!/bin/bash

#SBATCH --partition=multinode
#SBATCH --job-name=spark-multi-node
#SBATCH --exclusive

#Number of seperate nodes reserved for Spark cluster
#SBATCH --nodes=2
#SBATCH --cpus-per-task=12

#Number of excecution slots
#SBATCH --ntasks=2

#SBATCH --time=05:00:00
#SBATCH --mem=248g

# If your sys admin has installed spark as a module
module load spark

# If Spark is not installed as a module, you will need to specifiy absolute path to 
# $SPARK_HOME/bin/spark-start where $SPARK_HOME is on shared disk or at a consistant location
start-spark

echo $MASTER
sleep infinity
```
Submit the job file to Slurm:
```
sbatch run.cmd
```

This will start a Spark cluster containing two nodes that persists for five hours, unless you kill it sooner.
The file `slurm.out` created in the current directory will contain a line produced by `echo $MASTER` 
above which will indicate the address of the Spark master to which your application or ADAM-shell 
should connect such as `spark://somehostname:7077`

### Start adam-shell

Your sys admin will probably prefer that you launch your `adam-shell` or start an application from a 
cluster node rather than the head node you log in to. You may want to do so with:
```
sinteractive
```

Start an adam-shell:
```
$ADAM_HOME/bin/adam-shell --master spark://hostnamefromslurmdotout:7077
```

### or Run a batch job with adam-submit

```
$ADAM_HOME/bin/adam-submit --master spark://hostnamefromslurmdotout:7077
```

You should be able to connect to the Spark Web UI at `http://hostnamefromslurmdotout:4040`, however 
you may need to ask your local sys admin to open the requried ports.

### Feedback

We would love to hear feedback on your experience running ADAM on HPC/Slurm or other deployment architectures. 
Please let us know of any problems you run into via the mailing list or Gitter.
