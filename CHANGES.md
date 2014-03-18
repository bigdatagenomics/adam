# ADAM #

### Version parent ###
* ISSUE [178](https://github.com/bigdatagenomics/adam/pull/178): Upgrade to Hadoop-BAM 0.6.2/Picard 1.107
* ISSUE [166](https://github.com/bigdatagenomics/adam/pull/166): Pair-wise genotype concordance of genotype RDDs, with CLI tool
* ISSUE [171](https://github.com/bigdatagenomics/adam/pull/171): Add back in allele dosage for genotypes.

### Version 0.7.0 ###
* ISSUE [167](https://github.com/bigdatagenomics/adam/pull/167): Fix for Hadoop 1.0.x support
* ISSUE [165](https://github.com/bigdatagenomics/adam/pull/165): call PluginExecutor in apply method, fixes issue 164
* ISSUE [160](https://github.com/bigdatagenomics/adam/pull/160): Refactoring FASTA work to break contig sizes.
* ISSUE [78](https://github.com/bigdatagenomics/adam/pull/78): Upgrade to Spark 0.9 and Scala 2.10
* ISSUE [138](https://github.com/bigdatagenomics/adam/pull/138): Display Git commit info on command line
* ISSUE [161](https://github.com/bigdatagenomics/adam/pull/161): Added switches to spark context creation code
* ISSUE [117](https://github.com/bigdatagenomics/adam/pull/117): Add a "range join" method.
* ISSUE [151](https://github.com/bigdatagenomics/adam/pull/151): Vcf work concordance and genotype
* ISSUE [150](https://github.com/bigdatagenomics/adam/pull/150): Remaining variant changes for adam2vcf, unit tests, and CLI modifications
* ISSUE [147](https://github.com/bigdatagenomics/adam/pull/147): Resurrect VCF conversion code
* ISSUE [148](https://github.com/bigdatagenomics/adam/pull/148): Moving createSparkContext into core
* ISSUE [142](https://github.com/bigdatagenomics/adam/pull/142): Enforce Maven and Java versions
* ISSUE [144](https://github.com/bigdatagenomics/adam/pull/144): Merge of last few days of work on master into this branch
* ISSUE [124](https://github.com/bigdatagenomics/adam/pull/124): Vcf work rdd master merge
* ISSUE [143](https://github.com/bigdatagenomics/adam/pull/143): Changing package declaration to match test file location and removing un...
* ISSUE [140](https://github.com/bigdatagenomics/adam/pull/140): Update README.md
* ISSUE [139](https://github.com/bigdatagenomics/adam/pull/139): Update README.md
* ISSUE [129](https://github.com/bigdatagenomics/adam/pull/129): Modified pileup transforms to improve performance + to add options
* ISSUE [116](https://github.com/bigdatagenomics/adam/pull/116): add fastq interleaver script
* ISSUE [125](https://github.com/bigdatagenomics/adam/pull/125): Add design doc to CONTRIBUTING document
* ISSUE [114](https://github.com/bigdatagenomics/adam/pull/114): Changes to RDD utility files for new variant schema
* ISSUE [122](https://github.com/bigdatagenomics/adam/pull/122): Add IRC Channel to readme
* ISSUE [100](https://github.com/bigdatagenomics/adam/pull/100): CLI component changes for new variant schema
* ISSUE [108](https://github.com/bigdatagenomics/adam/pull/108): Adding new PluginExecutor command
* ISSUE [98](https://github.com/bigdatagenomics/adam/pull/98): Vcf work remove old variant
* ISSUE [104](https://github.com/bigdatagenomics/adam/pull/104): Added the port erasure to SparkFunSuite's cleanup.
* ISSUE [107](https://github.com/bigdatagenomics/adam/pull/107): Cleaning up change documentation.
* ISSUE [99](https://github.com/bigdatagenomics/adam/pull/99): Encoding tag types in the ADAMRecord attributes, adding the 'tags' command
* ISSUE [105](https://github.com/bigdatagenomics/adam/pull/105): Add initial documentation on contributing
* ISSUE [97](https://github.com/bigdatagenomics/adam/pull/97): New schema, variant context converter changes, and removal of old genoty...
* ISSUE [79](https://github.com/bigdatagenomics/adam/pull/79): Adding ability to convert reference FASTA files for nucleotide sequences
* ISSUE [91](https://github.com/bigdatagenomics/adam/pull/91): Minor change, increase adam-cli usage width to 150 characters
* ISSUE [86](https://github.com/bigdatagenomics/adam/pull/86): Fixes to pileup code
* ISSUE [88](https://github.com/bigdatagenomics/adam/pull/88): Added function for building variant context from genotypes.
* ISSUE [81](https://github.com/bigdatagenomics/adam/pull/81): Update README and cleanup top-level cli help text
* ISSUE [76](https://github.com/bigdatagenomics/adam/pull/76): Changing hadoop fs call to be compatible with Hadoop 1.
* ISSUE [74](https://github.com/bigdatagenomics/adam/pull/74): Updated CHANGES.txt to include note about the recursive-load branch.
* ISSUE [73](https://github.com/bigdatagenomics/adam/pull/73): Support for loading/combining multiple ADAM files into a single RDD.
* ISSUE [72](https://github.com/bigdatagenomics/adam/pull/72): Added ability to create regions from reads, and to merge adjacent regions
* ISSUE [71](https://github.com/bigdatagenomics/adam/pull/71): Change RecalTable to use optimized phred calculations
* ISSUE [68](https://github.com/bigdatagenomics/adam/pull/68): sonatype-nexus-snapshots repository is already in parent oss-parent-7 pom
* ISSUE [67](https://github.com/bigdatagenomics/adam/pull/67): fix for wildcard exclusion maven warnings
* ISSUE [65](https://github.com/bigdatagenomics/adam/pull/65): Create a cache for phred -> double values instead of recalculating
* ISSUE [60](https://github.com/bigdatagenomics/adam/pull/60): Bugfix for BQSR: Offset into qualityScore list was wrong
* ISSUE [66](https://github.com/bigdatagenomics/adam/pull/66): add pluginDependency section and remove versions in plugin sections
* ISSUE [61](https://github.com/bigdatagenomics/adam/pull/61): Filter utility for inverse of Projection
* ISSUE [48](https://github.com/bigdatagenomics/adam/pull/48): Fix read groups mapping and add Y as base type
* ISSUE [36](https://github.com/bigdatagenomics/adam/pull/36): Adding reads to rods transformation.
* ISSUE [56](https://github.com/bigdatagenomics/adam/pull/56): Adding Yy as base in MdTag

### Version 0.6.0 ###
* ISSUE [53](https://github.com/bigdatagenomics/adam/pull/53): Fix Hadoop 2.2.0 support, upgrade to Spark 0.8.1
* ISSUE [52](https://github.com/bigdatagenomics/adam/pull/52): Attributes: Use 't' instead of ',', as , is a valid character
* ISSUE [47](https://github.com/bigdatagenomics/adam/pull/47): Adding containsRefName to SequenceDictionary
* ISSUE [46](https://github.com/bigdatagenomics/adam/pull/46): Reduce logging for the actual adamSave job
* ISSUE [45](https://github.com/bigdatagenomics/adam/pull/45): Make MdTag immutable
* ISSUE [38](https://github.com/bigdatagenomics/adam/pull/38): Small bugfixes and cleanups to BQSR
* ISSUE [40](https://github.com/bigdatagenomics/adam/pull/40): Fixing reference position from offset implementation
* ISSUE [31](https://github.com/bigdatagenomics/adam/pull/31): Fixing a few issues in the ADAM2VCF2ADAM pipeline.
* ISSUE [30](https://github.com/bigdatagenomics/adam/pull/30): Suppress parquet logging in FieldEnumerationSuite
* ISSUE [28](https://github.com/bigdatagenomics/adam/pull/28): Fix build warnings
* ISSUE [24](https://github.com/bigdatagenomics/adam/pull/24): Add unit tests for marking duplicates
* ISSUE [26](https://github.com/bigdatagenomics/adam/pull/26): Fix unmapped reads in sequence dictionary
* ISSUE [23](https://github.com/bigdatagenomics/adam/pull/23): Generalizing the Projection class
* ISSUE [25](https://github.com/bigdatagenomics/adam/pull/25): Adding support for before, after clauses to SparkFunSuite.
* ISSUE [22](https://github.com/bigdatagenomics/adam/pull/22): Add a unit test for sorting reads
* ISSUE [21](https://github.com/bigdatagenomics/adam/pull/21): Adding rod functionality: a specialized grouping of pileup data.
* ISSUE [13](https://github.com/bigdatagenomics/adam/pull/13): Cleaning up VCF<->ADAM pipeline
* ISSUE [20](https://github.com/bigdatagenomics/adam/pull/20): Added Apache License 2.0 boilerplate to tops of all the GB-(c) files
* ISSUE [19](https://github.com/bigdatagenomics/adam/pull/19): Allow the Hadoop version to be specified
* ISSUE [17](https://github.com/bigdatagenomics/adam/pull/17): Fix transform -sort_reads partitioning. Add -coalesce option to transform.
* ISSUE [16](https://github.com/bigdatagenomics/adam/pull/16): Fixing an issue in pileup generation and in the MdTag util.
* ISSUE [15](https://github.com/bigdatagenomics/adam/pull/15): Tweaks 1
* ISSUE [12](https://github.com/bigdatagenomics/adam/pull/12): Subclass testing bug in AdamContext.adamLoad
* ISSUE [11](https://github.com/bigdatagenomics/adam/pull/11): Missing brackets in VcfConverter.getType
* ISSUE [10](https://github.com/bigdatagenomics/adam/pull/10): Moved record field name enum over to the projections package.
* ISSUE [8](https://github.com/bigdatagenomics/adam/pull/8): Fixes to sorting in ReferencePosition
* ISSUE [4](https://github.com/bigdatagenomics/adam/pull/4): New SparkFunSuite test support class, logging util and new BQSR test.
* ISSUE [1](https://github.com/bigdatagenomics/adam/pull/1): Fix scalatest configuration and fix unit tests
* ISSUE [14](https://github.com/bigdatagenomics/adam/pull/14): Converting some of the Option() calls to Some()
* ISSUE [13](https://github.com/bigdatagenomics/adam/pull/13): Cleaning up VCF<->ADAM pipeline
* ISSUE [9](https://github.com/bigdatagenomics/adam/pull/9): Adding support for a Sequence Dictionary from BAM files
* ISSUE [8](https://github.com/bigdatagenomics/adam/pull/8): Fixes to sorting in ReferencePosition
* ISSUE [7](https://github.com/bigdatagenomics/adam/pull/7): ADAM variant and genotype formats; and a VCF->ADAM converter
* ISSUE [4](https://github.com/bigdatagenomics/adam/pull/4): New SparkFunSuite test support class, logging util and new BQSR test.
* ISSUE [3](https://github.com/bigdatagenomics/adam/pull/3): Adding in implicit conversion functions for going between Java and Scala...
* ISSUE [2](https://github.com/bigdatagenomics/adam/pull/2): Update from Spark 0.7.3 to 0.8.0-incubating
* ISSUE [1](https://github.com/bigdatagenomics/adam/pull/1): Fix scalatest configuration and fix unit tests
