Using ADAM's Pipe API
---------------------

ADAM's ``GenomicDataset`` API provides support for piping the underlying
genomic data out to a single node process through the use of a ``pipe``
API. This builds off of Apache Spark's ``RDD.pipe`` API. However,
``RDD.pipe`` prints the objects as strings to the pipe. ADAM's pipe API
adds several important functions:

-  It supports on-the-fly conversions to widely used genomic file
   formats
-  It does not require input/output type matching (i.e., you can pipe
   reads in and get variants back from the pipe)
-  It adds the ability to set environment variables and to make local
   files (e.g., a reference genome) available to the run command
-  If the data are aligned, we ensure that each subcommand runs over a
   contiguous section of the reference genome, and that data are sorted
   on this chunk. We provide control over the size of any flanking
   region that is desired.

The method signature of a pipe command is below:

.. code:: scala

    def pipe[X, Y <: GenomicDataset[X, Y], V <: InFormatter[T, U, V]](cmd: Seq[String],
                                                                  files: Seq[String] = Seq.empty,
                                                                  environment: Map[String, String] = Map.empty,
                                                                  flankSize: Int = 0)(implicit tFormatterCompanion: InFormatterCompanion[T, U, V],
                                                                                      xFormatter: OutFormatter[X],
                                                                                      convFn: (U, RDD[X]) => Y,
                                                                                      tManifest: ClassTag[T],
                                                                                      xManifest: ClassTag[X]): Y

``X`` is the type of the records that are returned (e.g., for reads,
``Alignment``) and ``Y`` is the type of the ``GenomicDatset`` that is
returned (e.g., for reads, ``AlignmentDataset``). As explicit
parameters, we take:

-  ``cmd``: The command to run.
-  ``files``: Files to make available locally to each running command.
   These files can be referenced from ``cmd`` by using ``$#`` syntax,
   where ``#`` is the number of the file in the ``files`` sequence
   (e.g., ``$0`` is the head of the list, ``$1`` is the second file in
   the list, and so on).
-  ``environment``: Environment variable/value pairs to set locally for
   each running command.
-  ``flankSize``: The number of base pairs to flank each partition by,
   if piping genome aligned data.

Additionally, we take several important implicit parameters:

-  ``tFormatter``: The ``InFormatter`` that converts the data that is
   piped into the run command from the underlying ``GenomicDataset`` type.
-  ``xFormatter``: The ``OutFormatter`` that converts the data that is
   piped out of the run command back to objects for the output
   ``GenomicDataset``.
-  ``convFn``: A function that applies any necessary metadata
   conversions and creates a new ``GenomicDataset``.

The ``tManifest`` and ``xManifest`` implicit parameters are `Scala
ClassTag <http://www.scala-lang.org/api/2.10.3/index.html#scala.reflect.ClassTag>`__\ s
and will be provided by the compiler.

What are the implicit parameters used for? For each of the genomic
datatypes in ADAM, we support multiple legacy genomic filetypes (e.g.,
reads can be saved to or read from BAM, CRAM, FASTQ, and SAM). The
``InFormatter`` and ``OutFormatter`` parameters specify the format that
is being read into or out of the pipe. We support the following:

-  ``AlignmentDataset``:

   -  ``InFormatter``\ s: ``SAMInFormatter`` and ``BAMInFormatter`` write SAM or BAM out to a pipe.
   -  ``OutFormatter``: ``AnySAMOutFormatter`` supports reading SAM and BAM from a pipe, with the exact
      format autodetected from the stream.
   -  We do not support piping CRAM due to complexities around the reference-based compression.

-  ``FeatureDataset``:

   -  ``InFormatter``\ s: ``BEDInFormatter``, ``GFF3InFormatter``, ``GTFInFormatter``, and ``NarrowPeakInFormatter``
      for writing features out to a pipe in BED, GFF3, GTF/GFF2, or NarrowPeak format, respectively.
   -  ``OutFormatter``\ s: ``BEDOutFormatter``, ``GFF3OutFormatter``, ``GTFOutFormatter``, and ``NarrowPeakInFormatter``
      for reading features in BED, GFF3, GTF/GFF2, or NarrowPeak format in from a pipe, respectively.

-  ``FragmentDataset``:

   -  ``InFormatter``: ``InterleavedFASTQInFormatter`` writes FASTQ with the reads from a paired sequencing protocol
      interleaved in the FASTQ stream to a pipe.

-  ``VariantContextDataset``:
   -  ``InFormatter``: ``VCFInFormatter`` writes VCF to a pipe.
   -  ``OutFormatter``: ``VCFOutFormatter`` reads VCF from a pipe.

The ``convFn`` implementations are provided as implicit values in the
`ADAMContext <adamContext.html>`__. These conversion functions are needed
to adapt the metadata stored in a single ``GenomicDataset`` to the type of a
different ``GenomicDataset`` (e.g., if piping an ``AlignmentDataset``
through a command that returns a ``VariantContextDataset``, we will need to
convert the ``AlignmentDataset``\ s ``ReadGroupDictionary`` into an
array of ``Sample``\ s for the ``VariantContextDataset``). We provide four
implementations:

-  ``ADAMContext.sameTypeConversionFn``: For piped commands that do not
   change the type of the ``GenomicDataset`` (e.g., ``AlignmentDataset`` →
   ``AlignmentDataset``).
-  ``ADAMContext.readsToVCConversionFn``: For piped commands that go
   from an ``AlignmentDataset`` to a ``VariantContextDataset``.
-  ``ADAMContext.fragmentsToReadsConversionFn``: For piped commands that
   go from a ``FragmentDataset`` to an ``AlignmentDataset``.

To put everything together, here is an example command. Here, we will
run a command ``my_variant_caller``, which accepts one argument
``-R <reference>.fa``, SAM on standard input, and outputs VCF on
standard output:

.. code:: scala

    // import genomic dataset load functions and conversion functions
    import org.bdgenomics.adam.ds.ADAMContext._

    // import functionality for piping SAM into pipe
    import org.bdgenomics.adam.ds.read.SAMInFormatter

    // import functionality for reading VCF from pipe
    import org.bdgenomics.adam.converters.DefaultHeaderLines
    import org.bdgenomics.adam.ds.variant.{
      VariantContextDataset,
      VCFOutFormatter
    }

    // load the reads
    val reads = sc.loadAlignments("hdfs://mynamenode/my/read/file.bam")

    // define implicit informatter for sam
    implicit val tFormatter = SAMInFormatter

    // define implicit outformatter for vcf
    // attach all default headerlines
    implicit val uFormatter = new VCFOutFormatter(DefaultHeaderLines.allHeaderLines)

    // run the piped command
    // providing the explicit return type (VariantContextDataset) will ensure that
    // the correct implicit convFn is selected
    val variantContexts: VariantContextDataset = reads.pipe(
      cmd = Seq("my_variant_caller", "-R", "$0"),
      files = Seq("hdfs://mynamenode/my/reference/genome.fa"))

    // save to vcf
    variantContexts.saveAsVcf("hdfs://mynamenode/my/variants.vcf")

In this example, we assume that ``my_variant_caller`` is on the PATH on
each machine in our cluster. We suggest several different approaches:

-  Install the executable on the local filesystem of each machine on
   your cluster.
-  Install the executable on a shared file system (e.g., NFS) that is
   accessible from every machine in your cluster, and make sure that
   necessary prerequisites (e.g., python, dynamically linked libraries)
   are installed across each node on your cluster.
-  Run the command using a container system such as
   `Docker <https://docker.io>`__ or
   `Singularity <http://singularity.lbl.gov/>`__.

Using the Pipe API from Java
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pipe API example above uses Scala's implicit system and type
inference to make it easier to use the pipe API. However, we also
provide a Java equivalent. There are several changes:

-  The out-formatter is provided explicitly.
-  Instead of implicitly providing the companion object for the
   in-formatter, you provide the class of the in-formatter. This allows
   us to access the companion object via reflection.
-  For the conversion function, you can provide any function that
   implements the ``org.apache.spark.api.java.Function2`` interface. We
   provide common functions equivalent to those in ``ADAMContext`` in
   ``org.bdgenomics.adam.api.java.GenomicRDDConverters``.

To run the Scala example code above using Java, we would write:

.. code:: java

    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.List;
    import java.util.Map;
    import org.bdgenomics.adam.models.VariantContext
    import org.bdgenomics.adam.ds.read.AlignmentDataset;
    import org.bdgenomics.adam.ds.read.SAMInFormatter;
    import org.bdgenomics.adam.ds.variant.VariantContextDataset;
    import org.bdgenomics.adam.ds.variant.VCFOutFormatter;
    import org.bdgenomics.adam.api.java.AlignmentToVariantContextConverter;

    class PipeRunner {

      VariantContextDataset runPipe(AlignmentDataset reads) {

        List<String> cmd = new ArrayList<String>();
        cmd.add("my_variant_caller");
        cmd.add("-R");
        cmd.add("$0");

        List<String> files = new ArrayList<String>();
        files.add("hdfs://mynamenode/my/reference/genome.fa");

        Map<String, String> env = new HashMap<String, String>();

        return reads.pipe<VariantContext,
                          VariantContextDataset,
                          SAMInFormatter>(cmd,
                                          files,
                                          env,
                                          0,
                                          SAMInFormatter.class,
                                          new VCFOutFormatter,
                                          new AlignmentToVariantContextConverter);
      }
    }

Using the Pipe API from Python/R
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python and R follow the same calling style as the `Java pipe
API <#java-pipes>`__, but the in/out-formatter and conversion functions
are passed by name. We then use the classnames that are passed to the
function to create the objects via reflection. To run the example code
from above in Python, we would write:

.. code:: python

    from bigdatagenomics.adam.adamContext import ADAMContext

    ac = ADAMContext(self.sc)
    reads = ac.loadAlignments("hdfs://mynamenode/my/read/file.bam")

    variants = reads.pipe(["my_variant_caller", "-R", "$0"],
                          "org.bdgenomics.adam.ds.read.SAMInFormatter",
                          "org.bdgenomics.adam.ds.variant.VCFOutFormatter",
                          "org.bdgenomics.adam.api.java.AlignmentToVariantContextConverter",
                          files=[ "hdfs://mynamenode/my/reference/genome.fa" ])

In R, we would write:

.. code:: r

    library(bdg.adam)

    ac <- ADAMContext(sc)

    reads <- loadAlignments(ac, "hdfs://mynamenode/my/read/file.bam")

    cmd <- list("my_variant_caller", "-R", "$0")
    files <- list("hdfs://mynamenode/my/reference/genome.fa")

    variants <- pipe(reads,
                     cmd=cmd,
                     "org.bdgenomics.adam.ds.read.SAMInFormatter",
                     "org.bdgenomics.adam.ds.variant.VCFOutFormatter",
                     "org.bdgenomics.adam.api.java.AlignmentToVariantContextConverter",
                     files=files)

