# ADAM #

### Version 0.19.0 ###

**Closed issues:**

- Update bdg-utils dependency version to 0.2.4 [\#960](https://github.com/bigdatagenomics/adam/issues/960)
- Drop support for Spark version 1.2.1, Hadoop version 1.0.x [\#958](https://github.com/bigdatagenomics/adam/issues/958)
- Exception occurs when running tests on master [\#956](https://github.com/bigdatagenomics/adam/issues/956)
- Flagstat results still don't match samtools flagstat [\#946](https://github.com/bigdatagenomics/adam/issues/946)
- readInFragment value is not properly read from parquet file into RDD\[AlignmentRecord\] [\#942](https://github.com/bigdatagenomics/adam/issues/942)
- adam2vcf -sort\_on\_save flag broken [\#940](https://github.com/bigdatagenomics/adam/issues/940)
- Transform -limit\_projection requires .sam.seqdict file [\#937](https://github.com/bigdatagenomics/adam/issues/937)
- MarkDuplicates fails if library name is not set [\#934](https://github.com/bigdatagenomics/adam/issues/934)
- fastqtobam or sam [\#928](https://github.com/bigdatagenomics/adam/issues/928)
- Vcf2Adam uses SB field instead of FS field for fisher exact test for strand bias [\#923](https://github.com/bigdatagenomics/adam/issues/923)
- Add back limit\_projection on Transform [\#920](https://github.com/bigdatagenomics/adam/issues/920)
- BAM header is not getting set on partition 0 with headerless BAM output format [\#916](https://github.com/bigdatagenomics/adam/issues/916)
- Add numParts apply method to GenomicRegionPartitioner [\#914](https://github.com/bigdatagenomics/adam/issues/914)
- Add Spark version 1.6.x to Jenkins build matrix [\#913](https://github.com/bigdatagenomics/adam/issues/913)
- Target Spark 1.5.2 as default Spark version [\#911](https://github.com/bigdatagenomics/adam/issues/911)
- Move to bdg-formats 0.7.0 [\#905](https://github.com/bigdatagenomics/adam/issues/905)
- secondOfPair and firstOfPair flag is missing in the newest 0.18 adam transformed results from BAM [\#903](https://github.com/bigdatagenomics/adam/issues/903)
- Future pull request [\#900](https://github.com/bigdatagenomics/adam/issues/900)
- error in vcf2adam [\#899](https://github.com/bigdatagenomics/adam/issues/899)
- Importing directory of VCFs seems to fail [\#898](https://github.com/bigdatagenomics/adam/issues/898)
- How to filter genotypeRDD on sample names? org.apache.spark.SparkException: Task not serializable? [\#891](https://github.com/bigdatagenomics/adam/issues/891)
- Add Spark version 1.5.x to Jenkins build matrix [\#889](https://github.com/bigdatagenomics/adam/issues/889)
- Transform DAG causes stages to recompute [\#883](https://github.com/bigdatagenomics/adam/issues/883)
- adam-submit buildinfo is confused [\#880](https://github.com/bigdatagenomics/adam/issues/880)
- move\_to\_scala\_2.11 and maven-javadoc-plugin [\#863](https://github.com/bigdatagenomics/adam/issues/863)
- NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable [\#837](https://github.com/bigdatagenomics/adam/issues/837)
- Fix record oriented shuffle [\#599](https://github.com/bigdatagenomics/adam/issues/599)
- Avro.GenericData error with ADAM 0.12.0 on reading from ADAM file [\#290](https://github.com/bigdatagenomics/adam/issues/290)

**Merged and closed pull requests:**

- \[ADAM-960\] Updating bdg-utils dependency version to 0.2.4 [\#961](https://github.com/bigdatagenomics/adam/pull/961) ([heuermh](https://github.com/heuermh))
- \[ADAM-946\] Fixes to FlagStat for Samtools concordance issue [\#954](https://github.com/bigdatagenomics/adam/pull/954) ([jpdna](https://github.com/jpdna))
- Fix for travis build, replace reads2ref with reads2fragments [\#950](https://github.com/bigdatagenomics/adam/pull/950) ([heuermh](https://github.com/heuermh))
- \[ADAM-940\] Fix adam2vcf -sort_on_save flag [\#949](https://github.com/bigdatagenomics/adam/pull/949) ([massie](https://github.com/massie))
- Remove BuildInformation and extraneous git-commit-id-plugin configuration [\#948](https://github.com/bigdatagenomics/adam/pull/948) ([heuermh](https://github.com/heuermh))
- Update readme for spark 1.5.2 and hadoop 2.6.0 [\#944](https://github.com/bigdatagenomics/adam/pull/944) ([heuermh](https://github.com/heuermh))
- \[ADAM-942\] Replace first/secondInRead with readInFragment [\#943](https://github.com/bigdatagenomics/adam/pull/943) ([heuermh](https://github.com/heuermh))
- \[ADAM-937\] Adding check for aligned read predicate or limit projection flags and non-parquet input path [\#938](https://github.com/bigdatagenomics/adam/pull/938) ([heuermh](https://github.com/heuermh))
- \[ADAM-934\] Properly handle unset library name during duplicate marking [\#935](https://github.com/bigdatagenomics/adam/pull/935) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-911\] Move to Spark 1.5.2 and Hadoop 2.6.0 as default versions. [\#932](https://github.com/bigdatagenomics/adam/pull/932) ([fnothaft](https://github.com/fnothaft))
- added start and end values to Interval Trait. Used for IntervalRDD [\#931](https://github.com/bigdatagenomics/adam/pull/931) ([akmorrow13](https://github.com/akmorrow13))
- Removing buildinfo command [\#929](https://github.com/bigdatagenomics/adam/pull/929) ([heuermh](https://github.com/heuermh))
- Removing symbolic test resource links, read from test classpath instead [\#927](https://github.com/bigdatagenomics/adam/pull/927) ([heuermh](https://github.com/heuermh))
- Changed fisher strand bias field for VCF2Adam from SB to FS [\#924](https://github.com/bigdatagenomics/adam/pull/924) ([andrewmchen](https://github.com/andrewmchen))
- \[ADAM-920\] Limit tag/orig qual flags in Transform. [\#921](https://github.com/bigdatagenomics/adam/pull/921) ([fnothaft](https://github.com/fnothaft))
- Change the README to use adam-shell -i instead of pasting [\#919](https://github.com/bigdatagenomics/adam/pull/919) ([andrewmchen](https://github.com/andrewmchen))
- \[ADAM-916\] New strategy for writing header. [\#917](https://github.com/bigdatagenomics/adam/pull/917) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-914\] Create a GenomicRegionPartitioner given a partition count. [\#915](https://github.com/bigdatagenomics/adam/pull/915) ([fnothaft](https://github.com/fnothaft))
- Squashed \#907 and ran format-sources [\#908](https://github.com/bigdatagenomics/adam/pull/908) ([fnothaft](https://github.com/fnothaft))
- Various small fixes [\#907](https://github.com/bigdatagenomics/adam/pull/908) ([huitseeker](https://github.com/huitseeker))
- ADAM-599, 905: Move to bdg-formats:0.7.0 and migrate metadata [\#906](https://github.com/bigdatagenomics/adam/pull/906) ([fnothaft](https://github.com/fnothaft))
- Rewrote the getType method to handle all ploidy levels [\#904](https://github.com/bigdatagenomics/adam/pull/904) ([NeillGibson](https://github.com/NeillGibson))
- Single file save from \#733, rebased [\#901](https://github.com/bigdatagenomics/adam/pull/901) ([fnothaft](https://github.com/fnothaft))
- Added is\* genotype methods from HTS-JDK Genotype to RichGenotype [\#895](https://github.com/bigdatagenomics/adam/pull/895) ([NeillGibson](https://github.com/NeillGibson))
- \[ADAM-891\] Mark SparkContext as @transient. [\#894](https://github.com/bigdatagenomics/adam/pull/894) ([fnothaft](https://github.com/fnothaft))
- Update README URLs based on HTTP redirects [\#892](https://github.com/bigdatagenomics/adam/pull/892) ([ReadmeCritic](https://github.com/ReadmeCritic))
- adding --version command line option [\#888](https://github.com/bigdatagenomics/adam/pull/888) ([heuermh](https://github.com/heuermh))
- Add exception in move_to_scala_2.11.sh for maven-javadoc-plugin [\#887](https://github.com/bigdatagenomics/adam/pull/887) ([heuermh](https://github.com/heuermh))
- Fix tightlist bug in Pandoc [\#885](https://github.com/bigdatagenomics/adam/pull/885) ([massie](https://github.com/massie))
- \[ADAM-883\] Add caching to Transform pipeline. [\#884](https://github.com/bigdatagenomics/adam/pull/884) ([fnothaft](https://github.com/fnothaft))

### Version 0.18.2 ###
* ISSUE [877](https://github.com/bigdatagenomics/adam/pull/877): Minor fix to commit script to support https.
* ISSUE [876](https://github.com/bigdatagenomics/adam/pull/876): Separate command line argument words by underscores
* ISSUE [875](https://github.com/bigdatagenomics/adam/pull/875): P Operator parsing for MDTag
* ISSUE [873](https://github.com/bigdatagenomics/adam/pull/873): [ADAM-872] Modify regex to capture release and SNAPSHOT jars but not javadoc or sources jars
* ISSUE [866](https://github.com/bigdatagenomics/adam/pull/866): [ADAM-864] Don't force shuffle if reducing partition count.
* ISSUE [856](https://github.com/bigdatagenomics/adam/pull/856): export valid fastq
* ISSUE [847](https://github.com/bigdatagenomics/adam/pull/847): Updating build dependency versions to latest minor versions

### Version 0.18.1 ###
* ISSUE [870](https://github.com/bigdatagenomics/adam/pull/870): [ADAM-867] add pull requests missing from 0.18.0 release to CHANGES.md
* ISSUE [869](https://github.com/bigdatagenomics/adam/pull/869): [ADAM-868] make release branch and tag names consistent
* ISSUE [862](https://github.com/bigdatagenomics/adam/pull/862): [ADAM-861] use -d to check for repo assembly dir

### Version 0.18.0 ###
* ISSUE [860](https://github.com/bigdatagenomics/adam/pull/860): New release and pr-commit scripts
* ISSUE [859](https://github.com/bigdatagenomics/adam/pull/859): [ADAM-857] Corrected handling of env vars in bin scripts
* ISSUE [854](https://github.com/bigdatagenomics/adam/pull/854): [ADAM-853] allow main class in adam-submit to be specified
* ISSUE [852](https://github.com/bigdatagenomics/adam/pull/852): [ADAM-851] Slienced Parquet logging.
* ISSUE [850](https://github.com/bigdatagenomics/adam/pull/850): [ADAM-848] TwoBitFile now support nBlocks and maskBlocks
* ISSUE [846](https://github.com/bigdatagenomics/adam/pull/846): Updating maven build plugin dependency versions
* ISSUE [845](https://github.com/bigdatagenomics/adam/pull/845): [ADAM-780] Make DecadentRead package private.
* ISSUE [844](https://github.com/bigdatagenomics/adam/pull/844): [ADAM-843] Aggressively project out metadata fields.
* ISSUE [840](https://github.com/bigdatagenomics/adam/pull/840): fix flagstat output file encoding
* ISSUE [839](https://github.com/bigdatagenomics/adam/pull/839): let flagstat write to file
* ISSUE [831](https://github.com/bigdatagenomics/adam/pull/831): Support loading paired fastqs
* ISSUE [830](https://github.com/bigdatagenomics/adam/pull/830): better validation when saving paired fastqs
* ISSUE [829](https://github.com/bigdatagenomics/adam/pull/829): fix `Long != null` warnings
* ISSUE [819](https://github.com/bigdatagenomics/adam/pull/819): Implement custom ReferenceRegion hashcode
* ISSUE [816](https://github.com/bigdatagenomics/adam/pull/816): [ADAM-793] adding command to convert ADAM nucleotide contig fragments to FASTA files
* ISSUE [815](https://github.com/bigdatagenomics/adam/pull/815): Upgrade to bdg-formats:0.6.0, add Fragment datatype converters
* ISSUE [814](https://github.com/bigdatagenomics/adam/pull/814): [ADAM-812] fix for javadoc errors on JDK8
* ISSUE [813](https://github.com/bigdatagenomics/adam/pull/813): [ADAM-808] build an assembly cli jar with maven shade plugin
* ISSUE [810](https://github.com/bigdatagenomics/adam/pull/810): [ADAM-807] workaround for ktoso/maven-git-commit-id-plugin#61
* ISSUE [809](https://github.com/bigdatagenomics/adam/pull/809): [ADAM-785] Add support for all numeric array (TYPE=B) tags
* ISSUE [806](https://github.com/bigdatagenomics/adam/pull/806): [ADAM-755] updating utils dependency version to 0.2.3
* ISSUE [805](https://github.com/bigdatagenomics/adam/pull/805): Better transform error when file doesn't exist
* ISSUE [803](https://github.com/bigdatagenomics/adam/pull/803): fix unmapped-read sorting
* ISSUE [802](https://github.com/bigdatagenomics/adam/pull/802): stop writing contig names as md5 sums
* ISSUE [798](https://github.com/bigdatagenomics/adam/pull/798): fix SAM-attr conversion bug; int[]'s not byte[]'s
* ISSUE [790](https://github.com/bigdatagenomics/adam/pull/790): optionally add MDTags to reads with `transform`
* ISSUE [782](https://github.com/bigdatagenomics/adam/pull/782): Fix SAM Attribute parser for numeric array tags
* ISSUE [773](https://github.com/bigdatagenomics/adam/pull/773): [ADAM-772] fix some bash var quoting
* ISSUE [765](https://github.com/bigdatagenomics/adam/pull/765): [ADAM-752] Build for many combos of Spark/Hadoop versions.
* ISSUE [764](https://github.com/bigdatagenomics/adam/pull/764): More involved README restructuring
* ISSUE [762](https://github.com/bigdatagenomics/adam/pull/762): [ADAM-132] allowing list of commands to be injected into adam-cli ADAMMain

### Version 0.17.1 ###
* ISSUE [784](https://github.com/bigdatagenomics/adam/pull/784): [ADAM-783] Write @SQ header lines in sorted order.
* ISSUE [792](https://github.com/bigdatagenomics/adam/pull/792): [ADAM-791] Add repartition parameter to Fasta2ADAM.
* ISSUE [781](https://github.com/bigdatagenomics/adam/pull/781): [ADAM-777] Add validation stringency flag for BQSR.
* ISSUE [757](https://github.com/bigdatagenomics/adam/pull/757): We should print a warning message if the user has ADAM_OPTS set.
* ISSUE [770](https://github.com/bigdatagenomics/adam/pull/770): [ADAM-769] Fix serialization issue in known indel consensus model.
* ISSUE [763](https://github.com/bigdatagenomics/adam/pull/763): Clean up README links, other nits
* ISSUE [749](https://github.com/bigdatagenomics/adam/pull/749): Remove adam-cli jar from classpath during adam-submit
* ISSUE [754](https://github.com/bigdatagenomics/adam/pull/754): Bump ADAM to Spark 1.4
* ISSUE [753](https://github.com/bigdatagenomics/adam/pull/753): Bump Spark to 1.4
* ISSUE [748](https://github.com/bigdatagenomics/adam/pull/748): Fix for mdtag issues with insertions
* ISSUE [746](https://github.com/bigdatagenomics/adam/pull/746): Upgrade to Parquet 1.8.1.
* ISSUE [744](https://github.com/bigdatagenomics/adam/pull/744): [ADAM-743] exclude conflicting jackson dependencies
* ISSUE [737](https://github.com/bigdatagenomics/adam/pull/737): Reverse complement negative strand reads in fastq output
* ISSUE [731](https://github.com/bigdatagenomics/adam/pull/731): Fixed bug preventing use of TLEN attribute
* ISSUE [730](https://github.com/bigdatagenomics/adam/pull/730): [ADAM-729] Stuff TLEN into attributes.
* ISSUE [728](https://github.com/bigdatagenomics/adam/pull/728): [ADAM-709] Remove FeatureHierarchy and FeatureHierarchySuite
* ISSUE [719](https://github.com/bigdatagenomics/adam/pull/719): [ADAM-718] Use filesystem path to get underlying file system.
* ISSUE [712](https://github.com/bigdatagenomics/adam/pull/712): unify header-setting between BAM/SAM and VCF
* ISSUE [696](https://github.com/bigdatagenomics/adam/pull/696): include SequenceRecords from second-in-pair reads
* ISSUE [698](https://github.com/bigdatagenomics/adam/pull/698): class-ify ShuffleRegionJoin, force setting seqdict
* ISSUE [706](https://github.com/bigdatagenomics/adam/pull/706): restore clause guarding pruneCache check
* ISSUE [705](https://github.com/bigdatagenomics/adam/pull/705): GeneFeatureRDDFunctions → FeatureRDDFunctions

### Version 0.17.0 ###
* ISSUE [691](https://github.com/bigdatagenomics/adam/pull/691): fix BAM/SAM header setting when writing on cluster
* ISSUE [688](https://github.com/bigdatagenomics/adam/pull/688): make adamLoad public
* ISSUE [694](https://github.com/bigdatagenomics/adam/pull/694): Fix parent reference in distribution module
* ISSUE [684](https://github.com/bigdatagenomics/adam/pull/684): a few region-join nits
* ISSUE [682](https://github.com/bigdatagenomics/adam/pull/682): [ADAM-681] Remove menacing error message about reqd .adam extension
* ISSUE [680](https://github.com/bigdatagenomics/adam/pull/680): [ADAM-674] Delete Bam2ADAM.
* ISSUE [678](https://github.com/bigdatagenomics/adam/pull/678): upgrade to bdg utils 0.2.1
* ISSUE [668](https://github.com/bigdatagenomics/adam/pull/668): [ADAM-597] Move correction out of ADAM and into a downstream project.
* ISSUE [671](https://github.com/bigdatagenomics/adam/pull/671): Bug fix in ReferenceUtils.unionReferenceSet
* ISSUE [667](https://github.com/bigdatagenomics/adam/pull/667): [ADAM-666] Clean up key not found error in partitioner code.
* ISSUE [656](https://github.com/bigdatagenomics/adam/pull/656): Update Vcf2ADAM.scala
* ISSUE [652](https://github.com/bigdatagenomics/adam/pull/652): added filterByOverlappingRegion in GeneFeatureRDDFunctions
* ISSUE [650](https://github.com/bigdatagenomics/adam/pull/650): [ADAM-649] Support transform of all BAM/SAM files in a directory.
* ISSUE [647](https://github.com/bigdatagenomics/adam/pull/647): [ADAM-646] Special case reads with '*' quality during BQSR.
* ISSUE [645](https://github.com/bigdatagenomics/adam/pull/645): [ADAM-634] Create a local ParquetLister for testing purposes.
* ISSUE [633](https://github.com/bigdatagenomics/adam/pull/633): [Adam] Tests for SAMRecordConverter.scala
* ISSUE [641](https://github.com/bigdatagenomics/adam/pull/641): [ADAM-640] Fix incorrect exclusion for org.seqdoop.htsjdk.
* ISSUE [632](https://github.com/bigdatagenomics/adam/pull/632): [ADAM-631] Allow VCF conversion to sort on output after coalescing.
* ISSUE [628](https://github.com/bigdatagenomics/adam/pull/628): [ADAM-627] Makes ReferenceFile trait extend Serializable.
* ISSUE [637](https://github.com/bigdatagenomics/adam/pull/637): check for mac brew alternate spark install structure
* ISSUE [624](https://github.com/bigdatagenomics/adam/pull/624): Conceptual fix for duplicate marking and sorting stragglers
* ISSUE [629](https://github.com/bigdatagenomics/adam/pull/629): [ADAM-604] Remove normalization code.
* ISSUE [630](https://github.com/bigdatagenomics/adam/pull/630): Add flatten command.
* ISSUE [619](https://github.com/bigdatagenomics/adam/pull/619): [ADAM-540] Move to new HTSJDK release; should support Java 8.
* ISSUE [626](https://github.com/bigdatagenomics/adam/pull/626): [ADAM-625] Enable globbing for BAM.
* ISSUE [621](https://github.com/bigdatagenomics/adam/pull/621): Removes the predicates package.
* ISSUE [620](https://github.com/bigdatagenomics/adam/pull/620): [ADAM-600] Adding RegionJoin trait.
* ISSUE [616](https://github.com/bigdatagenomics/adam/pull/616): [ADAM-565] Upgrade to Parquet filter2 API.
* ISSUE [613](https://github.com/bigdatagenomics/adam/pull/613): [ADAM-612] Point to proper k-mer counters.
* ISSUE [588](https://github.com/bigdatagenomics/adam/pull/588): [ADAM-587] Clean up loading checks.
* ISSUE [592](https://github.com/bigdatagenomics/adam/pull/592): [ADAM-513] Remove ReferenceMappable trait.
* ISSUE [606](https://github.com/bigdatagenomics/adam/pull/606): [ADAM-605] Remove visualization code.
* ISSUE [596](https://github.com/bigdatagenomics/adam/pull/596): [ADAM-595] Delete the 'comparisons' code.
* ISSUE [590](https://github.com/bigdatagenomics/adam/pull/590): [ADAM-589] Removed pileup code.
* ISSUE [586](https://github.com/bigdatagenomics/adam/pull/586): [ADAM-452] Fixes SM attribute on ADAM to BAM conversion.
* ISSUE [584](https://github.com/bigdatagenomics/adam/pull/584): [ADAM-583] Add k-mer counting functionality for nucleotide contig fragments

### Version 0.16.0 ###
* ISSUE [570](https://github.com/bigdatagenomics/adam/pull/570): A few small conversion fixes
* ISSUE [579](https://github.com/bigdatagenomics/adam/pull/579): [ADAM-578] Update end of read when trimming.
* ISSUE [564](https://github.com/bigdatagenomics/adam/pull/564): [ADAM-563] Add warning message when saving Parquet files with incorrect extension
* ISSUE [576](https://github.com/bigdatagenomics/adam/pull/576): Changed hashCode implementations to improve performance of BQSR
* ISSUE [569](https://github.com/bigdatagenomics/adam/pull/569): Typo in the narrowPeak parser
* ISSUE [568](https://github.com/bigdatagenomics/adam/pull/568): Moved the Timers object from bdg-utils back to ADAM
* ISSUE [478](https://github.com/bigdatagenomics/adam/pull/478): Move non-genomics code
* ISSUE [550](https://github.com/bigdatagenomics/adam/pull/550): [ADAM-549] Added documentation for testing and CI for ADAM.
* ISSUE [555](https://github.com/bigdatagenomics/adam/pull/555): Makes maybeLoadVCF private.
* ISSUE [558](https://github.com/bigdatagenomics/adam/pull/558): Makes Features2ADAMSuite use SparkFunSuite
* ISSUE [557](https://github.com/bigdatagenomics/adam/pull/557): Randomize ports and turn off Spark UI to reduce bind exceptions in tests
* ISSUE [552](https://github.com/bigdatagenomics/adam/pull/552): Create test suite for FlagStat
* ISSUE [554](https://github.com/bigdatagenomics/adam/pull/554): privatize ADAMContext.maybeLoad{Bam,Fastq}
* ISSUE [551](https://github.com/bigdatagenomics/adam/pull/551): [ADAM-386] Multiline FASTQ input
* ISSUE [542](https://github.com/bigdatagenomics/adam/pull/542): Variants Visualization
* ISSUE [545](https://github.com/bigdatagenomics/adam/pull/545): [ADAM-543][ADAM-544] Fix issues with ADAM scripts and classpath
* ISSUE [535](https://github.com/bigdatagenomics/adam/pull/535): [ADAM-441] put a check in for Nothing. Throws an IAE if no return type is provided
* ISSUE [546](https://github.com/bigdatagenomics/adam/pull/546): [ADAM-532] Fix wigFix intermittent test failure
* ISSUE [534](https://github.com/bigdatagenomics/adam/pull/534): [ADAM-528][ADAM-533] Adds new RegionJoin impl that is shuffle-based
* ISSUE [531](https://github.com/bigdatagenomics/adam/pull/531): [ADAM-529] Attaching scaladoc to released distribution.
* ISSUE [413](https://github.com/bigdatagenomics/adam/pull/413): [ADAM-409][ADAM-520] Added local wigfix2bed tool
* ISSUE [527](https://github.com/bigdatagenomics/adam/pull/527): [ADAM-526] `VcfAnnotation2ADAM` only counts once
* ISSUE [523](https://github.com/bigdatagenomics/adam/pull/523): don't open non-.adam-extension files as ADAM files
* ISSUE [521](https://github.com/bigdatagenomics/adam/pull/521): quieting wget output
* ISSUE [482](https://github.com/bigdatagenomics/adam/pull/482): [ADAM-462] Coverage region calculation
* ISSUE [515](https://github.com/bigdatagenomics/adam/pull/515): [ADAM-510] fix for bash syntax error; add ADDL_JARS check to adam-submit

### Version 0.15.0 ###
* ISSUE [509](https://github.com/bigdatagenomics/adam/pull/509): Add a 'distribution' module to create assemblies
* ISSUE [508](https://github.com/bigdatagenomics/adam/pull/508): Upgrade from Parquet 1.4.3 to 1.6.0rc4
* ISSUE [498](https://github.com/bigdatagenomics/adam/pull/498): [ADAM-496] Changes VCF to flat ADAM command name and usage
* ISSUE [500](https://github.com/bigdatagenomics/adam/pull/500): [ADAM-495] Require SPARK_HOME for adam-submit
* ISSUE [501](https://github.com/bigdatagenomics/adam/pull/501): [ADAM-499] Add -onlyvariants option to vcf2adam
* ISSUE [507](https://github.com/bigdatagenomics/adam/pull/507): [ADAM-505] Removed `adam-local` from docs
* ISSUE [504](https://github.com/bigdatagenomics/adam/pull/504): [ADAM-502] Add missing Long implicit to ColumnReaderInput
* ISSUE [503](https://github.com/bigdatagenomics/adam/pull/503): [ADAM-473] Make RecordCondition and FieldCondition public
* ISSUE [494](https://github.com/bigdatagenomics/adam/pull/494): Fix foreach block for vcf ingest
* ISSUE [492](https://github.com/bigdatagenomics/adam/pull/492): Documentation cleanup and style improvements
* ISSUE [481](https://github.com/bigdatagenomics/adam/pull/481): [ADAM-480] Switch assembly to single goal.
* ISSUE [487](https://github.com/bigdatagenomics/adam/pull/487): [ADAM-486] Add port option to viz command.
* ISSUE [469](https://github.com/bigdatagenomics/adam/pull/469): [ADAM-461] Fix ReferenceRegion and ReferencePosition impl
* ISSUE [440](https://github.com/bigdatagenomics/adam/pull/440): [ADAM-439] Fix ADAM to account for BDG-FORMATS-35: Avro uses Strings
* ISSUE [470](https://github.com/bigdatagenomics/adam/pull/470): added ReferenceMapping for Genotype, filterByOverlappingRegion for GenotypeRDDFunctions
* ISSUE [468](https://github.com/bigdatagenomics/adam/pull/468): refactor RDD loading; explicitly load alignments
* ISSUE [474](https://github.com/bigdatagenomics/adam/pull/474): Consolidate documentation into a single location in source.
* ISSUE [471](https://github.com/bigdatagenomics/adam/pull/471): Fixed typo on MAVEN_OPTS quotation mark
* ISSUE [467](https://github.com/bigdatagenomics/adam/pull/467): [ADAM-436] Optionally output original qualities to fastq
* ISSUE [451](https://github.com/bigdatagenomics/adam/pull/451): add `adam view` command, analogous to `samtools view`
* ISSUE [466](https://github.com/bigdatagenomics/adam/pull/466): working examples on .sam included in repo
* ISSUE [458](https://github.com/bigdatagenomics/adam/pull/458): Remove unused val from Reads2Ref
* ISSUE [438](https://github.com/bigdatagenomics/adam/pull/438): Add ability to save paired-FASTQ files
* ISSUE [457](https://github.com/bigdatagenomics/adam/pull/457): A few random Predicate-related cleanups
* ISSUE [459](https://github.com/bigdatagenomics/adam/pull/459): a few tweaks to scripts/jenkins-test
* ISSUE [460](https://github.com/bigdatagenomics/adam/pull/460): Project only the sequence when kmer/qmer counting
* ISSUE [450](https://github.com/bigdatagenomics/adam/pull/450): Refactor some file writing and reading logic
* ISSUE [455](https://github.com/bigdatagenomics/adam/pull/455): [ADAM-454] Add serializers for Avro objects which don't have serializers
* ISSUE [447](https://github.com/bigdatagenomics/adam/pull/447): Update the contribution guidelines
* ISSUE [453](https://github.com/bigdatagenomics/adam/pull/453): Better null handling for isSameContig utility
* ISSUE [417](https://github.com/bigdatagenomics/adam/pull/417): Stores original position and original cigar during realignment.
* ISSUE [449](https://github.com/bigdatagenomics/adam/pull/449): read “OQ” attr from structured SAMRecord field
* ISSUE [446](https://github.com/bigdatagenomics/adam/pull/446): Revert "[ADAM-237] Migrate to Chill serialization libraries."
* ISSUE [437](https://github.com/bigdatagenomics/adam/pull/437): random nits
* ISSUE [434](https://github.com/bigdatagenomics/adam/pull/434): Few transform tweaks
* ISSUE [435](https://github.com/bigdatagenomics/adam/pull/435): [ADAM-403] Remove seqDict from RegionJoin
* ISSUE [431](https://github.com/bigdatagenomics/adam/pull/431): A few tweaks, typo corrections, and random cleanups
* ISSUE [430](https://github.com/bigdatagenomics/adam/pull/430): [ADAM-429] adam-submit now handles args correctly.
* ISSUE [427](https://github.com/bigdatagenomics/adam/pull/427): Fixes for indel realigner issues
* ISSUE [418](https://github.com/bigdatagenomics/adam/pull/418): [ADAM-416] Removing 'ADAM' prefix
* ISSUE [404](https://github.com/bigdatagenomics/adam/pull/404): [ADAM-327] Adding gene, transcript, and exon models.
* ISSUE [414](https://github.com/bigdatagenomics/adam/pull/414): Fix error in `adam-local` alias
* ISSUE [415](https://github.com/bigdatagenomics/adam/pull/415): Update README.md to reflect Spark 1.1
* ISSUE [412](https://github.com/bigdatagenomics/adam/pull/412): [ADAM-411] Updated usage aliases in README. Fixes #411.
* ISSUE [408](https://github.com/bigdatagenomics/adam/pull/408): [ADAM-405] Add FASTQ output.
* ISSUE [385](https://github.com/bigdatagenomics/adam/pull/385): [ADAM-384] Adds import from FASTQ.
* ISSUE [400](https://github.com/bigdatagenomics/adam/pull/400): [ADAM-399] Fix link to schemas.
* ISSUE [396](https://github.com/bigdatagenomics/adam/pull/396): [ADAM-388] Sets Kryo serialization with --conf args
* ISSUE [394](https://github.com/bigdatagenomics/adam/pull/394): [ADAM-393] Adds knobs to SparkContext creation in SparkFunSuite
* ISSUE [391](https://github.com/bigdatagenomics/adam/pull/391): [ADAM-237] Migrate to Chill serialization libraries.
* ISSUE [380](https://github.com/bigdatagenomics/adam/pull/380): Rewrite of MarkDuplicates which seems to improve performance
* ISSUE [387](https://github.com/bigdatagenomics/adam/pull/387): fix some deprecation warnings

### Version 0.14.0 ###
* ISSUE [376](https://github.com/bigdatagenomics/adam/pull/376): [ADAM-375] Upgrade to Hadoop-BAM 7.0.0.
* ISSUE [378](https://github.com/bigdatagenomics/adam/pull/378): [ADAM-360] Upgrade to Spark 1.1.0.
* ISSUE [379](https://github.com/bigdatagenomics/adam/pull/379): Fix the position of the jar path in the submit.
* ISSUE [383](https://github.com/bigdatagenomics/adam/pull/383): Make Mdtags handle '=' and 'X' cigar operators
* ISSUE [369](https://github.com/bigdatagenomics/adam/pull/369): [ADAM-369] Improve debug output for indel realigner
* ISSUE [377](https://github.com/bigdatagenomics/adam/pull/377): [ADAM-377] Update to Jenkins scripts and README.
* ISSUE [374](https://github.com/bigdatagenomics/adam/pull/374): [ADAM-372][ADAM-371][ADAM-365] Refactoring CLI to simplify and integrate with Spark model better
* ISSUE [370](https://github.com/bigdatagenomics/adam/pull/370): [ADAM-367] Updated alias in README.md
* ISSUE [368](https://github.com/bigdatagenomics/adam/pull/368): erasure, nonexhaustive-match, deprecation warnings
* ISSUE [354](https://github.com/bigdatagenomics/adam/pull/354): [ADAM-353] Fixing issue with SAM/BAM/VCF header attachment when running distributed
* ISSUE [357](https://github.com/bigdatagenomics/adam/pull/357): [ADAM-357] Added Java Plugin hook for ADAM.
* ISSUE [352](https://github.com/bigdatagenomics/adam/pull/352): Fix failing MD tag
* ISSUE [363](https://github.com/bigdatagenomics/adam/pull/363): Adding maven assembly plugin configuration to create tarballs
* ISSUE [364](https://github.com/bigdatagenomics/adam/pull/364): [ADAM-364] Fixing remaining cs.berkeley.edu URLs.
* ISSUE [362](https://github.com/bigdatagenomics/adam/pull/362): Remove mention of uberjar from README

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
