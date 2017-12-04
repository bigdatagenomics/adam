# Benchmarks {#benchmarks}

## Algorithm Performance {#algorithm-performance}

To test the efficiency of ADAM's implementations of the various algorithms that
are used in our variant calling pipeline, we ran strong scaling experiments. In
this experiment, we used the high coverage genome NA12878 from the 1000 Genomes
project. We held the executor configuration
constant, and stepped from one to four executors before doubling until we
reached 32 executors. Each executor had 16 hyperthreaded cores (32 concurrent
threads) and was allocated 200 gigabytes of memory.

In general, most of the algorithms in ADAM scale linearly as the
amount of compute resources is increased. The significant exception is BQSR,
which is explained by the large broadcast which runs during BQSR. The broadcast
introduces a serial step that runs for several minutes, as the multiple gigabyte
mask table is broadcast to all of the nodes in the cluster.
The figures below break these results down and compares them to
the latest implementation of these algorithms in the GATK4.

![Speedup when duplicate marking](source/img/speedup-md.pdf)
![Speedup when recalibrating base qualities](source/img/speedup-bqsr.pdf)
![Speedup when realigning INDELs](source/img/speedup-ir.pdf)

As our benchmarks demonstrate, ADAM's implementations of the various
algorithms in the standard germline reads-to-variants pipeline are more
performant than the same algorithms implemented in the GATK. ADAM outperforms the GATK when
running equivalent implementations of BQSR~(1.4$\times$ speedup) and duplicate
marking~(1.1$\times$ speedup). To improve the performance of duplicate marking,
we added a variant of the duplicate marking algorithm that operates on reads
that are grouped by sequencing fragment. By optimizing for reads that are
grouped with the other reads from their fragment, we can eliminate the first
shuffle in duplicate marking, which reassembles reads by read-pair. This
optimization is common in duplicate markers, as paired-end aligners will
typically emit a SAM or BAM file where consecutive reads are from the same
sequencing fragment.

## Storage Size {#storage}

ADAM uses [Apache Parquet](https://parquet.apache.org) as a way to store genomic
data. This is in addition to our support for conventional genomic file formats.
Parquet is an efficient columnar storage system that is widely used in the
analytics ecosystem, and integrates with a variety of data management tools and
query engines. Parquet provides improved storage capacity relative to several
conventional genomics data storage formats. Here, we look at the storage cost of
aligned reads, features, and variants.

### Aligned Reads {#aligned-reads-storage}

In this benchmark, we have stored a copy of NA12878 aligned to the GRCh37
reference genome using BWA. We store this genome in BAM, CRAM, and ADAM, using
the default compression settings for each. BAM and CRAM files were generated
using htslib. This read file was sequenced at approximately 60x coverage across
the whole genome.

![Storage cost of a 60x coverage WGS aligned dataset](source/img/bam.pdf)

ADAM provides a 20% improvement in storage size over BAM, while CRAM achieves
a 43% improvement in storage cost. CRAM achieves a higher compression
ratio by using reference-based compression techniques to minimize the amount
of data stored on disk.

### Features {#feature-storage}

Here, we benchmark both the GFF3 and BED formats. For GFF3, we use the ENSEMBL
GRCh38 genome annotation file. For BED, we use genome-wide coverage counts
generated from the NA12878 dataset used in the [aligned read
benchmarks](#aligned-reads-storage).

![Storage cost of genome annotations](source/img/gff.pdf)

For the genome annotation file, ADAM provides a 20% improvement in storage size
relative to the compressed GFF3 file.

![Storage cost of coverage data](source/img/bed.pdf)

For the coverage data, ADAM provides a 45% improvement in storage size relative
to the compressed BED file.

### Genomic Variants {#variant-storage}

In this benchmark, we used the 1,000 Genomes phase 3 data release VCFs. We
compared GZIP-compressed VCF and uncompressed VCF to ADAM.

![Storage cost of variant data](source/img/vcf.pdf)

Compressed VCF is approximately 10% smaller than genotype data stored as
Parquet.
