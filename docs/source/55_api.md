# API Overview {#api}

The main entrypoint to ADAM is the [ADAMContext](#adam-context), which allows genomic
data to be loaded in to Spark as [GenomicRDD](#genomic-rdd). GenomicRDDs can be transformed
using ADAM's built in [pre-processing algorithms](#algorithms), [Spark's RDD
primitives](#transforming), the [region join](#join) primitive, and ADAM's
[pipe](#pipes) APIs.


## Adding dependencies on ADAM libraries

ADAM libraries are available from [Maven Central](http://search.maven.org) under
the groupId `org.bdgenomics.adam`, such as the `adam-core` library:

```
<dependency>
  <groupId>org.bdgenomics.adam</groupId>
  <artifactId>adam-core${binary.version}</artifactId>
  <version>${adam.version}</version>
</dependency>
```

Scala apps should depend on `adam-core`, while Java applications should also depend
on `adam-apis`:

```
<dependency>
  <groupId>org.bdgenomics.adam</groupId>
  <artifactId>adam-apis${binary.version}</artifactId>
  <version>${adam.version}</version>
</dependency>
```

For each release, we support four `${binary.version}`s:

* `_2.10`: Spark 1.6.x on Scala 2.10
* `_2.11`: Spark 1.6.x on Scala 2.11
* `-spark2_2.10`: Spark 2.x on Scala 2.10
* `-spark2_2.11`: Spark 2.x on Scala 2.11

Additionally, we push nightly SNAPSHOT releases of ADAM to the
[Sonatype snapshot repo](https://oss.sonatype.org/content/repositories/snapshots/org/bdgenomics/adam/),
for developers who are interested in working on top of the latest changes in
ADAM.


## Loading data with the ADAMContext {#adam-context}

The ADAMContext is the main entrypoint to using ADAM. The ADAMContext wraps
an existing [SparkContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext)
to provide methods for loading genomic data. In Scala, we provide an implicit
conversion from a `SparkContext` to an `ADAMContext`. To use this, import the
implicit, and call an `ADAMContext` method:

```scala
import org.apache.spark.SparkContext

// the ._ at the end imports the implicit from the ADAMContext companion object
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

def loadReads(filePath: String, sc: SparkContext): AlignmentRecordRDD = {
  sc.loadAlignments(filePath)
}
```

In Java, instantiate a JavaADAMContext, which wraps an ADAMContext:

```java
import org.apache.spark.apis.java.JavaSparkContext;
import org.bdgenomics.adam.apis.java.JavaADAMContext
import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;

class LoadReads {

  public static AlignmentRecordRDD loadReads(String filePath,
                                             JavaSparkContext jsc) {
    // create an ADAMContext first
    ADAMContext ac = new ADAMContext(jsc.sc());
    
    // then wrap that in a JavaADAMContext
    JavaADAMContext jac = new JavaADAMContext(ac);

    return jac.loadAlignments(filePath);
  }
}
```

With an `ADAMContext`, you can load:

* Single reads as an `AlignmentRecordRDD`:
  * From SAM/BAM/CRAM using `loadBam` (Scala only)
  * Selected regions from an indexed BAM/CRAM using `loadIndexedBam` (Scala
    only)
  * From FASTQ using `loadFastq`, `loadPairedFastq`, and `loadUnpairedFastq`
    (Scala only)
  * From Parquet using `loadParquetAlignments` (Scala only)
  * The `loadAlignments` method will load from any of the above formats, and
    will autodetect the underlying format (Scala and Java, also supports loading
    reads from FASTA)
* Paired reads as a `FragmentRDD`:
  * From interleaved FASTQ using `loadInterleavedFastqAsFragments` (Scala only)
  * From Parquet using `loadParquetFragments` (Scala only)
  * The `loadFragments` method will load from either of the above formats, as
    well as SAM/BAM/CRAM, and will autodetect the underlying file format. If the
    file is a SAM/BAM/CRAM file and the file is queryname sorted, the data will
    be converted to fragments without performing a shuffle. (Scala and Java)
* VCF lines as a `VariantContextRDD` from VCF/BCF1 using `loadVcf` (Scala only)
* Selected lines from a tabix indexed VCF using `loadIndexedVcf` (Scala only)
* Genotypes as a `GenotypeRDD`:
  * From Parquet using `loadParquetGenotypes` (Scala only)
  * From either Parquet or VCF/BCF1 using `loadGenotypes` (Scala and Java)
* Variants as a `VariantRDD`:
  * From Parquet using `loadParquetVariants` (Scala only)
  * From either Parquet or VCF/BCF1 using `loadVariants` (Scala and Java)
* Genomic features as a `FeatureRDD`:
  * From BED using `loadBed` (Scala only)
  * From GFF3 using `loadGff3` (Scala only)
  * From GFF2/GTF using `loadGtf` (Scala only)
  * From NarrowPeak using `loadNarrowPeak` (Scala only)
  * From IntervalList using `loadIntervalList` (Scala only)
  * From Parquet using `loadParquetFeatures` (Scala only)
  * Autodetected from any of the above using `loadFeatures` (Scala and Java)
* Fragmented contig sequence as a `NucleotideContigFragmentRDD`:
  * From FASTA with `loadFasta` (Scala only)
  * From Parquet with `loadParquetContigFragments` (Scala only)
  * Autodetected from either of the above using `loadSequences` (Scala and Java)
* Coverage data as a `CoverageRDD`:
  * From Parquet using `loadParquetCoverage` (Scala only)
  * From Parquet or any of the feature file formats using `loadCoverage` (Scala
    only)
* Contig sequence as a broadcastable `ReferenceFile` using `loadReferenceFile`,
  which supports 2bit files, FASTA, and Parquet (Scala only)

The methods labeled "Scala only" may be usable from Java, but may not be
convenient to use.

The `JavaADAMContext` class provides Java-friendly methods that are equivalent
to the `ADAMContext` methods. Specifically, these methods use Java types, and do
not make use of default parameters. In addition to the load/save methods
described above, the `ADAMContext` adds the implicit methods needed for using
[ADAM's Pipe API](#pipes).


## Working with genomic data using GenomicRDDs {#genomic-rdd}

As described in the section on using the [ADAMContext}(#adam-context), ADAM
loads genomic data into a `GenomicRDD` which is specialized for each datatype.
This `GenomicRDD` wraps Apache Spark's Resilient Distributed Dataset (RDD,
[@zaharia12]) API with genomic metadata. The `RDD` abstraction presents an
array of data which is distributed across a cluster. `RDD`s are backed by
a computational lineage, which allows them to be recomputed if a node fails and
the results of a computation are lost. `RDD`s are processed by running
functional [transformations]{#transforming} across the whole dataset.

Around an `RDD`, ADAM adds metadata which describes the genome, samples, or
read group that a dataset came from. Specifically, ADAM supports the following
metadata:

* `GenomicRDD` base: A sequence dictionary, which describes the reference
  assembly that data is aligned to, if it is aligned. Applies to all types.
* `MultisampleGenomicRDD`: Adds metadata about the samples in a dataset.
  Applies to `GenotypeRDD`.
* `ReadGroupGenomicRDD`: Adds metadata about the read groups attached to a
  dataset. Applies to `AlignmentRecordRDD` and `FragmentRDD`.

Additionally, `GenotypeRDD`, `VariantRDD`, and `VariantContextRDD` store the
VCF header lines attached to the original file, to enable a round trip between
Parquet and VCF.

`GenomicRDD`s can be transformed several ways. These include:

* The [core preprocessing](#algorithms) algorithms in ADAM:
  * Reads:
    * Reads to coverage
    * [Recalibrate base qualities](#bqsr)
    * [INDEL realignment](#realignment)
    * [Mark duplicate reads](#duplicate-marking)
  * Fragments:
    * [Mark duplicate fragments](#duplicate-marking)
* [RDD transformations](#transforming)
* [By using ADAM to pipe out to another tool](#pipes)

### Transforming GenomicRDDs {#transforming}

Although `GenomicRDD`s do not extend Apache Spark's `RDD` class, `RDD`
operations can be performed on them using the `transform` method. Currently,
we only support `RDD` to `RDD` transformations that keep the same type as
the base type of the `GenomicRDD`. To apply an `RDD` transform, use the
`transform` method, which takes a function mapping one `RDD` of the base
type into another `RDD` of the base type. For example, we could use `transform`
on an `AlignmentRecordRDD` to filter out reads that have a low mapping quality,
but we cannot use `transform` to translate those reads into `Feature`s showing
the genomic locations covered by reads.


## Using ADAM’s RegionJoin API {#join}

Another useful API implemented in ADAM is the RegionJoin API, which joins two
genomic datasets that contain overlapping regions. This primitive is useful for
a number of applications including variant calling (identifying all of the reads 
that overlap a candidate variant), coverage analysis (determining the coverage 
depth for each region in a reference), and INDEL realignment (identify INDELs 
aligned against a reference).

There are two overlap join implementations available in ADAM: 
BroadcastRegionJoin and ShuffleRegionJoin. The result of a ShuffleRegionJoin 
is identical to the BroadcastRegionJoin, however they serve different 
purposes depending on the content of the two datasets.

The ShuffleRegionJoin is at its core a distributed sort-merge overlap join. 
To ensure that the data is appropriately colocated, we perform a copartition 
on the right dataset before the each node conducts the join locally. The 
BroadcastRegionJoin performs an overlap join by broadcasting a copy of the 
entire left dataset to each node. ShuffleRegionJoin should be used if the right 
dataset is too large to send to all nodes and both datasets have low 
cardinality. The BroadcastRegionJoin should be used when you are joining a 
smaller dataset to a larger one and/or the datasets in the join have high 
cardinality.

Another important distinction between ShuffleRegionJoin and 
BroadcastRegionJoin is the join operations available in ADAM. See the table
below for an exact list of what joins are available for each type of
RegionJoin.

To perform a ShuffleRegionJoin, use the following:

```scala
dataset1.shuffleRegionJoin(dataset2)
```

To perform a BroadcastRegionJoin, use the following:

```scala
dataset1.broadcastRegionJoin(dataset2)
```

Where `dataset1` and `dataset2` are `GenomicRDD`s. If you used the ADAMContext to 
read a genomic dataset into memory, this condition is met.

ADAM has a variety of ShuffleRegionJoin types that you can perform on your 
data, and all are called in a similar way:

![Joins Available]
(img/join_examples.png)


Join call | action | Availability
----------|--------|
```dataset1.shuffleRegionJoin(dataset2) ``` ```dataset1.broadcastRegionJoin(dataset2)```| perform an inner join | ShuffleRegionJoin BroadcastRegionJoin
```dataset1.fullOuterShuffleRegionJoin(datset2)```|perform an outer join | ShuffleRegionJoin
```dataset1.leftOuterShuffleRegionJoin(dataset2)```|perform a left outer join | ShuffleRegionJoin
```dataset1.rightOuterShuffleRegionJoin(dataset2)``` ```dataset1.rightOuterBroadcastRegionJoin(dataset2)```|perform a right outer join | ShuffleRegionJoin BroadcastRegionJoin
```dataset1.shuffleRegionJoinAndGroupByLeft(dataset2)``` |perform an inner join and group joined values by the records on the left | ShuffleRegionJoin
```dataset1.broadcastRegionJoinAndGroupByRight(dataset2)``` | perform an inner join and group joined values by the records on the right | ShuffleRegionJoin
```dataset1.rightOuterShuffleRegionJoinAndGroupByLeft(dataset2)```|perform a right outer join and group joined values by the records on the left | ShuffleRegionJoin
```rightOuterBroadcastRegionJoinAndGroupByRight``` | perform a right outer join and group joined values by the records on the right | BroadcastRegionJoin

One common pattern involves joining a single dataset against many datasets. An
example of this is joining an RDD of features (e.g., gene/exon coordinates)
against many different RDD's of reads. If the object that is being used many
times (gene/exon coordinates, in this case), we can force that object to be
broadcast once and reused many times with the `broadcast()` function. This
pairs with the `broadcastRegionJoin` and `rightOuterBroadcastRegionJoin`
functions. For example, given the following code:

```scala
val reads = sc.loadAlignments("my/reads.adam")
val panel = sc.loadFeatures("my/panel/features.adam")

val readsByFeature = panel.broadcastRegionJoin(reads)
```

We can get a handle to the broadcast panel by rewriting the code as:

```scala
val reads = sc.loadAlignments("my/reads.adam")
val bcastPanel = sc.loadFeatures("my/panel/features.adam").broadcast()

val readsByFeature = reads.broadcastRegionJoinAgainst(bcastPanel)
```

## Using ADAM's Pipe API {#pipes}

ADAM's `GenomicRDD` API provides support for piping the underlying genomic data
out to a single node process through the use of a `pipe` API. This builds off of
Apache Spark's `RDD.pipe` API. However, `RDD.pipe` prints the objects as
strings to the pipe. ADAM's pipe API adds several important functions:

* It supports on-the-fly conversions to widely used genomic file formats
* It doesn't require input/output type matching (i.e., you can pipe reads in
  and get variants back from the pipe)
* It adds the ability to set environment variables and to make local files
  (e.g., a reference genome) available to the run command
* If the data is aligned, we ensure that each subcommand runs over a contiguious
  section of the reference genome, and that data is sorted on this chunk. We
  provide control over the size of any flanking region that is desired.

The method signature of a pipe command is below:

```scala
def pipe[X, Y <: GenomicRDD[X, Y], V <: InFormatter[T, U, V]](cmd: String,
                                                              files: Seq[String] = Seq.empty,
                                                              environment: Map[String, String] = Map.empty,
                                                              flankSize: Int = 0)(implicit tFormatterCompanion: InFormatterCompanion[T, U, V],
                                                                                  xFormatter: OutFormatter[X],
                                                                                  convFn: (U, RDD[X]) => Y,
                                                                                  tManifest: ClassTag[T],
                                                                                  xManifest: ClassTag[X]): Y
```

`X` is the type of the records that are returned (e.g., for reads,
`AlignmentRecord`) and `Y` is the type of the `GenomicRDD` that is returned
(e.g., for reads, `AlignmentRecordRDD`). As explicit parameters, we take:

* `cmd`: The command to run.
* `files`: Files to make available locally to each running command. These files
  can be referenced from `cmd` by using `$#` syntax, where `#` is the number
  of the file in the `files` sequence (e.g., `$0` is the head of the list,
  `$1` is the second file in the list, and so on).
* `environment`: Environment variable/value pairs to set locally for each
  running command.
* `flankSize`: The number of base pairs to flank each partition by, if piping
  genome aligned data.

Additionally, we take several important implicit parameters:

* `tFormatter`: The `InFormatter` that converts the data that is piped into the
  run command from the underlying `GenomicRDD` type.
* `xFormatter`: The `OutFormatter` that converts the data that is piped out of
  the run command back to objects for the output `GenomicRDD`.
* `convFn`: A function that applies any necessary metadata conversions and
  creates a new `GenomicRDD`.

The `tManifest` and `xManifest` implicit parameters are [Scala
ClassTag](http://www.scala-lang.org/api/2.10.3/index.html#scala.reflect.ClassTag)s
and will be provided by the compiler.

What are the implicit parameters used for? For each of the genomic datatypes in
ADAM, we support multiple legacy genomic filetypes (e.g., reads can be saved to
or read from BAM, CRAM, FASTQ, and SAM). The `InFormatter` and `OutFormatter`
parameters specify the format that is being read into or out of the pipe. We
support the following:

* `AlignmentRecordRDD`:
  * `InFormatter`s: `SAMInFormatter` and `BAMInFormatter` write SAM or BAM out
    to a pipe.
  * `OutFormatter`: `AnySAMOutFormatter` supports reading SAM and BAM from a
    pipe, with the exact format autodetected from the stream.
  * We do not support piping CRAM due to complexities around the reference-based
    compression.
* `FeatureRDD`:
  * `InForamtter`s: `BEDInFormatter`, `GFF3InFormatter`, `GTFInFormatter`, and
  `NarrowPeakInFormatter` for writing features out to a pipe in BED, GFF3, GTF/GFF2,
  or NarrowPeak format, respectively.
  * `OutFormatter`s: `BEDOutFormatter`, `GFF3OutFormatter`, `GTFOutFormatter`, and
  `NarrowPeakInFormatter` for reading features in BED, GFF3, GTF/GFF2, or NarrowPeak
  format in from a pipe, respectively.
* `FragmentRDD`:
  * `InFormatter`: `InterleavedFASTQInFormatter` writes FASTQ with the reads
    from a paired sequencing protocol interleaved in the FASTQ stream to a pipe.
* `VariantContextRDD`:
  * `InFormatter`: `VCFInFormatter` writes VCF to a pipe.
  * `OutFormatter`: `VCFOutFormatter` reads VCF from a pipe.

The `convFn` implementations are provided as implicit values in the
[ADAMContext](#adam-context). These conversion functions are needed to adapt
the metadata stored in a single `GenomicRDD` to the type of a different
`GenomicRDD` (e.g., if piping an `AlignmentRecordRDD` through a command that
returns a `VariantContextRDD`, we will need to convert the `AlignmentRecordRDD`s
`RecordGroupDictionary` into an array of `Sample`s for the `VariantContextRDD`).
We provide four implementations:

* `ADAMContext.sameTypeConversionFn`: For piped commands that do not change the
  type of the `GenomicRDD` (e.g., `AlignmentRecordRDD` &rarr; `AlignmentRecordRDD`).
* `ADAMContext.readsToVCConversionFn`: For piped commands that go from an
  `AlignmentRecordRDD` to a `VariantContextRDD`.
* `ADAMContext.fragmentsToReadsConversionFn`: For piped commands that go from a
  `FragmentRDD` to an `AlignmentRecordRDD`.

To put everything together, here's an example command. Here, we will run a
command `my_variant_caller`, which accepts one argument `-R <reference>.fa`,
SAM on standard input, and outputs VCF on standard output:

```scala
// import RDD load functions and conversion functions
import org.bdgenomics.adam.rdd.ADAMContext._

// import functionality for piping SAM into pipe
import org.bdgenomics.adam.rdd.read.SAMInFormatter

// import functionality for reading VCF from pipe
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.rdd.variant.{
  VariantContextRDD,
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
// providing the explicit return type (VariantContextRDD) will ensure that
// the correct implicit convFn is selected
val variantContexts: VariantContextRDD = reads.pipe("my_variant_caller -R $0",
  files = Seq("hdfs://mynamenode/my/reference/genome.fa"))

// save to vcf
variantContexts.saveAsVcf("hdfs://mynamenode/my/variants.vcf")
```

In this example, we assume that `my_variant_caller` is on the PATH on each
machine in our cluster. We suggest several different approaches:

* Install the executable on the local filesystem of each machine on your
  cluster.
* Install the executable on a shared file system (e.g., NFS) that is accessible
  from every machine in your cluster, and make sure that necessary prerequisites
  (e.g., python, dynamically linked libraries) are installed across each node on
  your cluster.
* Run the command using a container system such as [Docker](https://docker.io)
  or [Singularity](http://singularity.lbl.gov/).
