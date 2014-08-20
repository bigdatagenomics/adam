# ADAM #

### Version 0.13.0 ###
* ISSUE [343](https://github.com/bigdatagenomics/adam/pull/343): Allow retrying on failure for HTTPRangedByteAccess
* ISSUE [349](https://github.com/bigdatagenomics/adam/pull/349): Fix for a NullPointerException when hostname is null in Task Metrics
* ISSUE [347](https://github.com/bigdatagenomics/adam/pull/347): Bug fix for genome browser
* ISSUE [346](https://github.com/bigdatagenomics/adam/pull/346): Genome visualization
* ISSUE [342](https://github.com/bigdatagenomics/adam/pull/342): [ADAM-309] Update to bdg-formats 0.2.0
* ISSUE [333](https://github.com/bigdatagenomics/adam/pull/333): [ADAM-332] Upgrades ADAM to Spark 1.0.1.
* ISSUE [341](https://github.com/bigdatagenomics/adam/pull/341): [ADAM-340] Adding the TrackedLayout trait and implementation.
* ISSUE [337](https://github.com/bigdatagenomics/adam/pull/337): [ADAM-335] Updated README.md to reflect migration to appassembler.
* ISSUE [311](https://github.com/bigdatagenomics/adam/pull/311): Adding several simple normalizations.
* ISSUE [330](https://github.com/bigdatagenomics/adam/pull/330): Make mismatch and deletes positions accessible
* ISSUE [334](https://github.com/bigdatagenomics/adam/pull/334): Moving code coverage into a profile
* ISSUE [329](https://github.com/bigdatagenomics/adam/pull/329): Add count of mismatches to mdtag
* ISSUE [328](https://github.com/bigdatagenomics/adam/pull/328): [ADAM-326] Adding a 5-second retry on the HttpRangedByteAccess test.
* ISSUE [325](https://github.com/bigdatagenomics/adam/pull/325): Adding documentation for commit/issue nomenclature and rebasing

### Version 0.12.1 ###
* ISSUE [308](https://github.com/bigdatagenomics/adam/pull/308): Fixing the 'index 0' bug in features2adam
* ISSUE [306](https://github.com/bigdatagenomics/adam/pull/306): Adding code for lifting over between sequences and the reference genome.
* ISSUE [320](https://github.com/bigdatagenomics/adam/pull/320): Remove extraneous implicit methods in ReferenceMappingContext
* ISSUE [314](https://github.com/bigdatagenomics/adam/pull/314): Updates to indel realigner to improve performance and accuracy.
* ISSUE [319](https://github.com/bigdatagenomics/adam/pull/319): Adding scripts for publishing scaladoc.
* ISSUE [315](https://github.com/bigdatagenomics/adam/pull/315): Added table of (wall-clock) stage durations when print_metrics is used
* ISSUE [312](https://github.com/bigdatagenomics/adam/pull/312): Fixing sources jar
* ISSUE [313](https://github.com/bigdatagenomics/adam/pull/313): Making the CredentialsProperties file optional
* ISSUE [267](https://github.com/bigdatagenomics/adam/pull/267): Parquet and indexed Parquet RDD implementations, and indices.
* ISSUE [301](https://github.com/bigdatagenomics/adam/pull/301): Add Beacon's AlleleCount
* ISSUE [293](https://github.com/bigdatagenomics/adam/pull/293): Add aggregation and display of metrics obtained from Spark
* ISSUE [295](https://github.com/bigdatagenomics/adam/pull/295): Fix broken link to ADAM specification for storing reads.
* ISSUE [292](https://github.com/bigdatagenomics/adam/pull/292): Cleaning up scaladoc generation warnings.
* ISSUE [289](https://github.com/bigdatagenomics/adam/pull/289): Modifying interleaved fastq format to be hadoop version independent.
* ISSUE [288](https://github.com/bigdatagenomics/adam/pull/288): Add ADAMFeature to Kryo registrator
* ISSUE [286](https://github.com/bigdatagenomics/adam/pull/286): Removing some debug printout that was left in.
* ISSUE [287](https://github.com/bigdatagenomics/adam/pull/287): Cleaning hadoop dependencies
* ISSUE [285](https://github.com/bigdatagenomics/adam/pull/285): Refactoring read groups to increase the amount of data stored.
* ISSUE [284](https://github.com/bigdatagenomics/adam/pull/284): Cleaning up build warnings.
* ISSUE [280](https://github.com/bigdatagenomics/adam/pull/280): Move to bdg-formats
* ISSUE [283](https://github.com/bigdatagenomics/adam/pull/283): Fix reference name comment
* ISSUE [282](https://github.com/bigdatagenomics/adam/pull/282): Minor cleanup on interleaved FASTQ input format.
* ISSUE [277](https://github.com/bigdatagenomics/adam/pull/277): Implemented HTTPRangedByteAccess.
* ISSUE [274](https://github.com/bigdatagenomics/adam/pull/274): Added clarifying note to `ADAMVariantContext`
* ISSUE [279](https://github.com/bigdatagenomics/adam/pull/279): Simplify format-source
* ISSUE [278](https://github.com/bigdatagenomics/adam/pull/278): Use maven license plugin to ensure source has correct license
* ISSUE [268](https://github.com/bigdatagenomics/adam/pull/268): Adding fixed depth prefix trie implementation
* ISSUE [273](https://github.com/bigdatagenomics/adam/pull/273): Fixes issue in reference models where strings are not sanitized on collection from avro.
* ISSUE [272](https://github.com/bigdatagenomics/adam/pull/272): Created command categories
* ISSUE [269](https://github.com/bigdatagenomics/adam/pull/269): Adding k-mer and q-mer counting.
* ISSUE [271](https://github.com/bigdatagenomics/adam/pull/271): Consolidate Parquet logging configuration

### Version 0.12.0 ###
* ISSUE [264](https://github.com/bigdatagenomics/adam/pull/264): Parquet-related Utility Classes
* ISSUE [259](https://github.com/bigdatagenomics/adam/pull/259): ADAMFlatGenotype is a smaller, flat version of a genotype schema
* ISSUE [266](https://github.com/bigdatagenomics/adam/pull/266): Removed extra command 'BuildInformation'
* ISSUE [263](https://github.com/bigdatagenomics/adam/pull/263): Added AdamContext.referenceLengthFromCigar
* ISSUE [260](https://github.com/bigdatagenomics/adam/pull/260): Modifying conversion code to resolve #112.
* ISSUE [258](https://github.com/bigdatagenomics/adam/pull/258): Adding an 'args' parameter to the plugin framework.
* ISSUE [262](https://github.com/bigdatagenomics/adam/pull/262): Adding reference assembly name to ADAMContig.
* ISSUE [256](https://github.com/bigdatagenomics/adam/pull/256): Upgrading to Spark 1.0
* ISSUE [257](https://github.com/bigdatagenomics/adam/pull/257): Adds toString method for sequence dictionary.
* ISSUE [255](https://github.com/bigdatagenomics/adam/pull/255): Add equals, canEqual, and hashCode methods to MdTag class

### Version 0.11.0 ###
* ISSUE [254](https://github.com/bigdatagenomics/adam/pull/254): Cleanup import statements
* ISSUE [250](https://github.com/bigdatagenomics/adam/pull/250): Adding ADAM to SAM conversion.
* ISSUE [248](https://github.com/bigdatagenomics/adam/pull/248): Adding utilities for read trimming.
* ISSUE [252](https://github.com/bigdatagenomics/adam/pull/252): Added a note about rebasing-off-master to CONTRIBUTING.md
* ISSUE [249](https://github.com/bigdatagenomics/adam/pull/249): Cosmetic changes to FastaConverter and FastaConverterSuite.
* ISSUE [251](https://github.com/bigdatagenomics/adam/pull/251): CHANGES.md is updated at release instead of per pull request
* ISSUE [247](https://github.com/bigdatagenomics/adam/pull/247): For #244, Fragments were incorrect order and incomplete
* ISSUE [246](https://github.com/bigdatagenomics/adam/pull/246): Making sample ID field in genotype nullable.
* ISSUE [245](https://github.com/bigdatagenomics/adam/pull/245): Adding ADAMContig back to ADAMVariant.
* ISSUE [243](https://github.com/bigdatagenomics/adam/pull/243): Rebase PR#238 onto master

### Version 0.10.0 ###
* ISSUE [242](https://github.com/bigdatagenomics/adam/pull/242): Upgrade to Parquet 1.4.3
* ISSUE [241](https://github.com/bigdatagenomics/adam/pull/241): Fixes to FASTA code to properly handle indices.
* ISSUE [239](https://github.com/bigdatagenomics/adam/pull/239): Make ADAMVCFOutputFormat public
* ISSUE [233](https://github.com/bigdatagenomics/adam/pull/233): Build up reference information during cigar processing
* ISSUE [234](https://github.com/bigdatagenomics/adam/pull/234): Predicate to filter conversion
* ISSUE [235](https://github.com/bigdatagenomics/adam/pull/235): Remove unused contiglength field
* ISSUE [232](https://github.com/bigdatagenomics/adam/pull/232): Add `-pretty` and `-o` to the `print` command
* ISSUE [230](https://github.com/bigdatagenomics/adam/pull/230): Remove duplicate mdtag field
* ISSUE [231](https://github.com/bigdatagenomics/adam/pull/231): Helper scripts to run an ADAM Console.
* ISSUE [226](https://github.com/bigdatagenomics/adam/pull/226): Fix ReferenceRegion from ADAMRecord
* ISSUE [225](https://github.com/bigdatagenomics/adam/pull/225): Change Some to Option to check for unmapped reads
* ISSUE [223](https://github.com/bigdatagenomics/adam/pull/223): Use SparkConf object to configure SparkContext
* ISSUE [217](https://github.com/bigdatagenomics/adam/pull/217): Stop using reference IDs and use reference names instead
* ISSUE [220](https://github.com/bigdatagenomics/adam/pull/220): Update SAM to ADAM conversion
* ISSUE [213](https://github.com/bigdatagenomics/adam/pull/213): BQSR updates

### Version 0.9.0 ###
* ISSUE [214](https://github.com/bigdatagenomics/adam/pull/214): Upgrade to Spark 0.9.1
* ISSUE [211](https://github.com/bigdatagenomics/adam/pull/211): FastaConverter Refactor
* ISSUE [212](https://github.com/bigdatagenomics/adam/pull/212): Cleanup build warnings
* ISSUE [210](https://github.com/bigdatagenomics/adam/pull/210): Remove Scalariform from process-sources phase
* ISSUE [209](https://github.com/bigdatagenomics/adam/pull/209): Fix Scalariform issues and Maven warnings
* ISSUE [207](https://github.com/bigdatagenomics/adam/pull/207): Change from deprecated manifest erasure to runtimeClass
* ISSUE [206](https://github.com/bigdatagenomics/adam/pull/206): Add Scalariform settings to pom
* ISSUE [204](https://github.com/bigdatagenomics/adam/pull/204): Update Avro code gen to not mark fields as deprecated.

### Version 0.8.0 ###
* ISSUE [203](https://github.com/bigdatagenomics/adam/pull/203): Move package from edu.berkeley.cs.amplab to org.bdgenomics
* ISSUE [199](https://github.com/bigdatagenomics/adam/pull/199): Updating pileup conversion code to convert sequences that use the X and = (EQ) CIGAR operators
* ISSUE [191](https://github.com/bigdatagenomics/adam/pull/191): Add repartition parameter
* ISSUE [183](https://github.com/bigdatagenomics/adam/pull/183): Fixing Job.getInstance call that breaks hadoop 1 compatibility.
* ISSUE [192](https://github.com/bigdatagenomics/adam/pull/192): Add docs and scripts for creating a release
* ISSUE [193](https://github.com/bigdatagenomics/adam/pull/193): Issue #137, clarify role of CHANGES.{md,txt}

### Version 0.7.2 ###
* ISSUE [187](https://github.com/bigdatagenomics/adam/pull/187): Add summarize_genotypes command
* ISSUE [178](https://github.com/bigdatagenomics/adam/pull/178): Upgraded to Hadoop-BAM 0.6.2/Picard 1.107.
* ISSUE [173](https://github.com/bigdatagenomics/adam/pull/173): Parse annotations out of vcf files
* ISSUE [162](https://github.com/bigdatagenomics/adam/pull/162): Refactored SequenceDictionary
* ISSUE [180](https://github.com/bigdatagenomics/adam/pull/180): BQSR using vcf loader
* ISSUE [179](https://github.com/bigdatagenomics/adam/pull/179): Update maven-surefire-plugin dependency version to 2.17, also create an ...
* ISSUE [175](https://github.com/bigdatagenomics/adam/pull/175): VariantContext converter refactor
* ISSUE [169](https://github.com/bigdatagenomics/adam/pull/169): Cleaning up mpileup command
* ISSUE [170](https://github.com/bigdatagenomics/adam/pull/170): Adding variant field enumerations

### Version 0.7.1 ###

### Version 0.7.3 ###

### Version 0.7.2 ###
* ISSUE [166](https://github.com/bigdatagenomics/adam/pull/166): Pair-wise genotype concordance of genotype RDDs, with CLI tool

### Version 0.7.0 ###
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
