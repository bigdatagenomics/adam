# ADAM #

### Version 0.21.0 ###

**Closed issues:**

- Update Markdown docs with ValidationStringency in VCF<->ADAM CLI [\#1342](https://github.com/bigdatagenomics/adam/issues/1342)
- Variant VCFHeaderLine metadata does not handle wildcards properly [\#1339](https://github.com/bigdatagenomics/adam/issues/1339)
- Close called multiple times on VCF header stream [\#1337](https://github.com/bigdatagenomics/adam/issues/1337)
- BroadcastRegionJoin has serialization failures [\#1334](https://github.com/bigdatagenomics/adam/issues/1334)
- adam-cli uses git-commit-id-plugin which breaks release? [\#1322](https://github.com/bigdatagenomics/adam/issues/1322)
- move_to_xyz scripts should have interlocks... [\#1317](https://github.com/bigdatagenomics/adam/issues/1317)
- Lineage for partitionAndJoin in ShuffleRegionJoin causes StackOverflow Errors [\#1308](https://github.com/bigdatagenomics/adam/issues/1308)
- Add move_to_spark_1.sh script and update README to mention [\#1307](https://github.com/bigdatagenomics/adam/issues/1307)
- adam-submit transform fails with Exception in thread "main" java.lang.IncompatibleClassChangeError: Implementing class [\#1306](https://github.com/bigdatagenomics/adam/issues/1306)
- private ADAMContext constructor? [\#1296](https://github.com/bigdatagenomics/adam/issues/1296)
- AlignmentRecord.mateAlignmentEnd never set [\#1290](https://github.com/bigdatagenomics/adam/issues/1290)
- how to submit my own driver class via adam-submit? [\#1289](https://github.com/bigdatagenomics/adam/issues/1289)
- ReferenceRegion on Genotype seems busted? [\#1286](https://github.com/bigdatagenomics/adam/issues/1286)
- Clarify strandedness in ReferenceRegion apply methods [\#1285](https://github.com/bigdatagenomics/adam/issues/1285)
- Parquet and CRAM debug logging during unit tests [\#1280](https://github.com/bigdatagenomics/adam/issues/1280)
- Add more ANN field parsing unit tests [\#1273](https://github.com/bigdatagenomics/adam/issues/1273)
- loadVariantAnnotations returns empty RDD [\#1271](https://github.com/bigdatagenomics/adam/issues/1271)
- Implement joinVariantAnnotations with region join [\#1259](https://github.com/bigdatagenomics/adam/issues/1259)
- Count how many chromosome in the range of the kmer [\#1249](https://github.com/bigdatagenomics/adam/issues/1249)
- ADAM minor release to support htsjdk 2.7.0? [\#1248](https://github.com/bigdatagenomics/adam/issues/1248)
- how to config kryo.registrator programmatically [\#1245](https://github.com/bigdatagenomics/adam/issues/1245)
- Does the nested record Flattener drop Maps/Arrays? [\#1244](https://github.com/bigdatagenomics/adam/issues/1244)
- Dead-ish code cleanup in `org.bdgenomics.adam.utils` [\#1242](https://github.com/bigdatagenomics/adam/issues/1242)
- java.io.FileNotFoundException for old adam file after upgrade to adam0.20 [\#1240](https://github.com/bigdatagenomics/adam/issues/1240)
- please add maven-source-plugin into the pom file [\#1239](https://github.com/bigdatagenomics/adam/issues/1239)
- Assembly jar doesn't get rebuilt on CLI changes [\#1238](https://github.com/bigdatagenomics/adam/issues/1238)
- how to compare with the last the column for the same chromosome name? [\#1237](https://github.com/bigdatagenomics/adam/issues/1237)
- Need a way for users to add VCF header lines [\#1233](https://github.com/bigdatagenomics/adam/issues/1233)
- Enhancements to VCF save [\#1232](https://github.com/bigdatagenomics/adam/issues/1232)
- Must we split multi-allelic sites in our Genotype model? [\#1231](https://github.com/bigdatagenomics/adam/issues/1231)
- Can't override default -collapse in reads2coverage [\#1228](https://github.com/bigdatagenomics/adam/issues/1228)
- Reads2coverage NPEs on unmapped reads [\#1227](https://github.com/bigdatagenomics/adam/issues/1227)
- Strand bias doesn't get exported [\#1226](https://github.com/bigdatagenomics/adam/issues/1226)
- Move ADAMFunSuite helper functions upstream to SparkFunSuite [\#1225](https://github.com/bigdatagenomics/adam/issues/1225)
- broadcast join using interval tree [\#1224](https://github.com/bigdatagenomics/adam/issues/1224)
- Instrumentation is lost in ShuffleRegionJoin [\#1222](https://github.com/bigdatagenomics/adam/issues/1222)
- Bump Spark, Scala, Hadoop dependency versions [\#1221](https://github.com/bigdatagenomics/adam/issues/1221)
- GenomicRDD shuffle region join passes partition count to partition size [\#1220](https://github.com/bigdatagenomics/adam/issues/1220)
- Scala compile errors downstream of Spark 2 Scala 2.11 artifacts [\#1218](https://github.com/bigdatagenomics/adam/issues/1218)
- Javac error: incompatible types: SparkContext cannot be converted to ADAMContext [\#1217](https://github.com/bigdatagenomics/adam/issues/1217)
- Release 0.20.0 artifacts failed Sonatype Nexus validation [\#1212](https://github.com/bigdatagenomics/adam/issues/1212)
- Release script failed for 0.20.0 release [\#1211](https://github.com/bigdatagenomics/adam/issues/1211)
- gVCF - can't load multi-allelic sites [\#1202](https://github.com/bigdatagenomics/adam/issues/1202)
- Allow open-ended intervals in loadIndexedBam [\#1196](https://github.com/bigdatagenomics/adam/issues/1196)
- Interval tree join in ADAM [\#1171](https://github.com/bigdatagenomics/adam/issues/1171)
- spark-submit throw exception in spark-standalone using .adam which transformed from .vcf [\#1121](https://github.com/bigdatagenomics/adam/issues/1121)
- BroadcastRegionJoin is not a broadcast join [\#1110](https://github.com/bigdatagenomics/adam/issues/1110)
- Improve test coverage of VariantContextConverter [\#1107](https://github.com/bigdatagenomics/adam/issues/1107)
- Variant dbsnp rs id tracking in vcf2adam and ADAM2Vcf [\#1103](https://github.com/bigdatagenomics/adam/issues/1103)
- Document core ADAM transform methods [\#1085](https://github.com/bigdatagenomics/adam/issues/1085)
- Document deploying ADAM on Toil [\#1084](https://github.com/bigdatagenomics/adam/issues/1084)
- Clean up packages [\#1083](https://github.com/bigdatagenomics/adam/issues/1083)
- VariantCallingAnnotations is getting populated with INFO fields [\#1063](https://github.com/bigdatagenomics/adam/issues/1063)
- How to load DatabaseVariantAnnotation information ? [\#1049](https://github.com/bigdatagenomics/adam/issues/1049)
- Release ADAM version 0.20.0 [\#1048](https://github.com/bigdatagenomics/adam/issues/1048)
- Support VCF annotation ANN field in vcf2adam and adam2vcf [\#1044](https://github.com/bigdatagenomics/adam/issues/1044)
- How to create a rich(er) VariantContext RDD? Reconstruct VCF INFO fields. [\#878](https://github.com/bigdatagenomics/adam/issues/878)
- Add biologist targeted section to the README [\#497](https://github.com/bigdatagenomics/adam/issues/497)
- Update usage docs running for EC2 and CDH [\#493](https://github.com/bigdatagenomics/adam/issues/493)
- Add docs about building downstream apps on top of ADAM [\#291](https://github.com/bigdatagenomics/adam/issues/291)
- Variant filter representation [\#194](https://github.com/bigdatagenomics/adam/issues/194)

**Merged and closed pull requests:**

- [ADAM-1342] Update CLI docs after #1288 merged. [\#1343](https://github.com/bigdatagenomics/adam/pull/1343) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1339] Use glob-safe method to load VCF header metadata for Parquet [\#1340](https://github.com/bigdatagenomics/adam/pull/1340) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1337] Remove os.{flush,close} calls after writing VCF header. [\#1338](https://github.com/bigdatagenomics/adam/pull/1338) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1334] Clean up serialization issues in Broadcast region join. [\#1336](https://github.com/bigdatagenomics/adam/pull/1336) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1307] move_to_spark_2 fails after moving to scala 2.11. [\#1329](https://github.com/bigdatagenomics/adam/pull/1329) ([fnothaft](https://github.com/fnothaft))
- unroll/optimize some JavaConversions [\#1326](https://github.com/bigdatagenomics/adam/pull/1326) ([ryan-williams](https://github.com/ryan-williams))
- clean up *Join type-params/scaldocs [\#1325](https://github.com/bigdatagenomics/adam/pull/1325) ([ryan-williams](https://github.com/ryan-williams))
- [ADAM-1322] Skip git commit plugin if .git is missing. [\#1323](https://github.com/bigdatagenomics/adam/pull/1323) ([fnothaft](https://github.com/fnothaft))
- Supports access to indexed fa and fasta files [\#1320](https://github.com/bigdatagenomics/adam/pull/1320) ([akmorrow13](https://github.com/akmorrow13))
- Add interlocks for move_to_xyz scripts. [\#1319](https://github.com/bigdatagenomics/adam/pull/1319) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1307] Add script for moving to Spark 1. [\#1318](https://github.com/bigdatagenomics/adam/pull/1318) ([fnothaft](https://github.com/fnothaft))
- Update move_to_spark_2.sh [\#1316](https://github.com/bigdatagenomics/adam/pull/1316) ([creggian](https://github.com/creggian))
- [ADAM-1308] Fix stack overflow in join with custom iterator impl. [\#1315](https://github.com/bigdatagenomics/adam/pull/1315) ([fnothaft](https://github.com/fnothaft))
- Why Adam? section added to README.md [\#1310](https://github.com/bigdatagenomics/adam/pull/1310) ([tverbeiren](https://github.com/tverbeiren)) 
- Add docs about using ADAM's Kryo registrator from another Kryo registrator. [\#1305](https://github.com/bigdatagenomics/adam/pull/1305) ([fnothaft](https://github.com/fnothaft))
- Add docs about building downstream applications [\#1304](https://github.com/bigdatagenomics/adam/pull/1304) ([heuermh](https://github.com/heuermh))
- [ADAM-493] Add ADAM-on-Spark-on-YARN docs. [\#1301](https://github.com/bigdatagenomics/adam/pull/1301) ([fnothaft](https://github.com/fnothaft))
- Code style fixes [\#1299](https://github.com/bigdatagenomics/adam/pull/1299) ([heuermh](https://github.com/heuermh))
- Make ADAMContext and JavaADAMContext constructors public [\#1298](https://github.com/bigdatagenomics/adam/pull/1298) ([heuermh](https://github.com/heuermh))
- Remove back reference between VariantAnnotation and Variant [\#1297](https://github.com/bigdatagenomics/adam/pull/1297) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1280] Silence CRAM logging in tests. [\#1294](https://github.com/bigdatagenomics/adam/pull/1294) ([fnothaft](https://github.com/fnothaft))
- HBase as a separate repo [\#1293](https://github.com/bigdatagenomics/adam/pull/1293) ([jpdna](https://github.com/jpdna))
- Reference region cleanup [\#1291](https://github.com/bigdatagenomics/adam/pull/1291) ([fnothaft](https://github.com/fnothaft))
- Clean rewrite of VariantContextConverter [\#1288](https://github.com/bigdatagenomics/adam/pull/1288) ([fnothaft](https://github.com/fnothaft))
- add function:filterByOverlappingRegions [\#1287](https://github.com/bigdatagenomics/adam/pull/1287) ([liamlee](https://github.com/liamlee)) 
- Populate fields on VariantAnnotation [\#1283](https://github.com/bigdatagenomics/adam/pull/1283) ([heuermh](https://github.com/heuermh))
- Add VCF headers for fields in Variant and VariantAnnotation records [\#1281](https://github.com/bigdatagenomics/adam/pull/1281) ([heuermh](https://github.com/heuermh))
- CGCloud deploy docs [\#1279](https://github.com/bigdatagenomics/adam/pull/1279) ([jpdna](https://github.com/jpdna))
- some style nits [\#1278](https://github.com/bigdatagenomics/adam/pull/1278) ([ryan-williams](https://github.com/ryan-williams))
- use ParsedLoci in loadIndexedBam [\#1277](https://github.com/bigdatagenomics/adam/pull/1277) ([ryan-williams](https://github.com/ryan-williams))
- Increasing unit test coverage for VariantContextConverter [\#1276](https://github.com/bigdatagenomics/adam/pull/1276) ([heuermh](https://github.com/heuermh))
- Expose FeatureRDD to public [\#1275](https://github.com/bigdatagenomics/adam/pull/1275) ([Georgehe4](https://github.com/Georgehe4)) 
- Clean up CLI operation categories and names, and add documentation for CLI [\#1274](https://github.com/bigdatagenomics/adam/pull/1274) ([fnothaft](https://github.com/fnothaft))
- Rename org.bdgenomics.adam.rdd.variation package to o.b.a.rdd.variant [\#1270](https://github.com/bigdatagenomics/adam/pull/1270) ([heuermh](https://github.com/heuermh))
- use testFile in some tests [\#1268](https://github.com/bigdatagenomics/adam/pull/1268) ([ryan-williams](https://github.com/ryan-williams))
- [ADAM-1083] Cleaning up `org.bdgenomics.adam.models`. [\#1267](https://github.com/bigdatagenomics/adam/pull/1267) ([fnothaft](https://github.com/fnothaft))
- make py file py3-forward-compatible [\#1266](https://github.com/bigdatagenomics/adam/pull/1266) ([ryan-williams](https://github.com/ryan-williams))
- rm accidentally-added file [\#1265](https://github.com/bigdatagenomics/adam/pull/1265) ([fnothaft](https://github.com/fnothaft))
- Finishing up the cleanup on org.bdgenomics.adam.rdd. [\#1264](https://github.com/bigdatagenomics/adam/pull/1264) ([fnothaft](https://github.com/fnothaft))
- Clean up `org.bdgenomics.adam.rich` package. [\#1263](https://github.com/bigdatagenomics/adam/pull/1263) ([fnothaft](https://github.com/fnothaft))
- Add docs for transform pipeline, ADAM-on-Toil [\#1262](https://github.com/bigdatagenomics/adam/pull/1262) ([fnothaft](https://github.com/fnothaft))
- updates for bdg utils 0.2.9-SNAPSHOT [\#1261](https://github.com/bigdatagenomics/adam/pull/1261) ([akmorrow13](https://github.com/akmorrow13))
- [ADAM-1233] Expose header lines in Variant-related GenomicRDDs [\#1260](https://github.com/bigdatagenomics/adam/pull/1260) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1221] Bump Spark/Hadoop versions. [\#1258](https://github.com/bigdatagenomics/adam/pull/1258) ([fnothaft](https://github.com/fnothaft))
- Rename org.bdgenomics.adam.rdd.features package to o.b.a.rdd.feature [\#1256](https://github.com/bigdatagenomics/adam/pull/1256) ([heuermh](https://github.com/heuermh))
- Clean up documentation in `org.bdgenomics.adam.projection`. [\#1255](https://github.com/bigdatagenomics/adam/pull/1255) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1221] Bump Spark/Hadoop versions. [\#1254](https://github.com/bigdatagenomics/adam/pull/1254) ([fnothaft](https://github.com/fnothaft))
- Misc shuffle join fixes. [\#1253](https://github.com/bigdatagenomics/adam/pull/1253) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1196] Add support for open ReferenceRegions. [\#1252](https://github.com/bigdatagenomics/adam/pull/1252) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1225] Move helper functions from ADAMFunSuite to SparkFunSuite. [\#1251](https://github.com/bigdatagenomics/adam/pull/1251) ([fnothaft](https://github.com/fnothaft))
- Merge VariantAnnotation and DatabaseVariantAnnotation records [\#1250](https://github.com/bigdatagenomics/adam/pull/1250) ([heuermh](https://github.com/heuermh))
- Miscellaneous VCF fixes [\#1247](https://github.com/bigdatagenomics/adam/pull/1247) ([fnothaft](https://github.com/fnothaft))
- HBase backend for Genotypes [\#1246](https://github.com/bigdatagenomics/adam/pull/1246) ([jpdna](https://github.com/jpdna))
- [ADAM-1242] Clean up dead code in org.bdgenomics.adam.util. [\#1243](https://github.com/bigdatagenomics/adam/pull/1243) ([fnothaft](https://github.com/fnothaft))
- Small cleanup of "replacing uses of deprecated class SAMFileReader" [\#1236](https://github.com/bigdatagenomics/adam/pull/1236) ([fnothaft](https://github.com/fnothaft))
- replacing uses of deprecated class SAMFileReader [\#1235](https://github.com/bigdatagenomics/adam/pull/1235) ([lbergelson](https://github.com/lbergelson))
- [ADAM-1224] Replace BroadcastRegionJoin with tree based algo. [\#1234](https://github.com/bigdatagenomics/adam/pull/1234) ([fnothaft](https://github.com/fnothaft))
- Fix reads2coverage issues [\#1230](https://github.com/bigdatagenomics/adam/pull/1230) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1212] Add empty assembly object, allows Maven build to create sources and javadoc artifacts [\#1215](https://github.com/bigdatagenomics/adam/pull/1215) ([heuermh](https://github.com/heuermh))
- [ADAM-1211] Fix call to move_to_scala_2.sh, reorder Spark 2.x Scala 2.10 and 2.10 sections [\#1214](https://github.com/bigdatagenomics/adam/pull/1214) ([heuermh](https://github.com/heuermh))
- demonstrate multi-allelic gVCF failure - test added [\#1205](https://github.com/bigdatagenomics/adam/pull/1205) ([jpdna](https://github.com/jpdna))
- Merge VariantAnnotation and DatabaseVariantAnnotation records [\#1144](https://github.com/bigdatagenomics/adam/pull/1144) ([heuermh](https://github.com/heuermh))
- Upgrade to bdg-formats-0.10.0 [\#1135](https://github.com/bigdatagenomics/adam/pull/1135) ([fnothaft](https://github.com/fnothaft))

### Version 0.20.0 ###

**Closed issues:**

- Sorting by reference index seems doesn't work or sorted by DESC order? [\#1204](https://github.com/bigdatagenomics/adam/issues/1204)
- master won't compile [\#1200](https://github.com/bigdatagenomics/adam/issues/1200)
- VCF format tag SB field parse error in loading [\#1199](https://github.com/bigdatagenomics/adam/issues/1199)
- Publish sources JAR with snapshots [\#1195](https://github.com/bigdatagenomics/adam/issues/1195)
- Type SparkFunSuite in package org.bdgenomics.utils.misc is not available [\#1193](https://github.com/bigdatagenomics/adam/issues/1193)
- MDTagging fails on GRCh38 [\#1192](https://github.com/bigdatagenomics/adam/issues/1192)
- Fix stack overflow in IndelRealigner serialization [\#1190](https://github.com/bigdatagenomics/adam/issues/1190)
- Delete `./scripts/commit-pr.sh` [\#1188](https://github.com/bigdatagenomics/adam/issues/1188)
- Hadoop globStatus returns null if no glob matches [\#1186](https://github.com/bigdatagenomics/adam/issues/1186)
- Swapping out IntervalRDD under GenomicRDDs [\#1184](https://github.com/bigdatagenomics/adam/issues/1184)
- How to get "SO coordinate" instead of "SO unsorted"? [\#1182](https://github.com/bigdatagenomics/adam/issues/1182)
- How to read glob of multiple parquet Genotype [\#1179](https://github.com/bigdatagenomics/adam/issues/1179)
- Update command line doc and examples in README.md [\#1176](https://github.com/bigdatagenomics/adam/issues/1176)
- FastqRecordConverter needs cleanup and tests [\#1172](https://github.com/bigdatagenomics/adam/issues/1172)
- TransformFormats write to .gff3 file path incorrectly writes as parquet [\#1168](https://github.com/bigdatagenomics/adam/issues/1168)
- Should be able to merge shards across two different file systems [\#1165](https://github.com/bigdatagenomics/adam/issues/1165)
- RG ID gets written as the index, not the record group name [\#1162](https://github.com/bigdatagenomics/adam/issues/1162)
- Users should be able to save files as `-single` without merging them [\#1161](https://github.com/bigdatagenomics/adam/issues/1161)
- Users should be able to set size of buffer used for merging files [\#1160](https://github.com/bigdatagenomics/adam/issues/1160)
- Bump Hadoop-BAM to 7.7.0 [\#1158](https://github.com/bigdatagenomics/adam/issues/1158)
- adam-shell prints command trace to stdout [\#1154](https://github.com/bigdatagenomics/adam/issues/1154)
- Map IntervalList format column four to feature name or attributes? [\#1152](https://github.com/bigdatagenomics/adam/issues/1152)
- Parquet storage of VariantContext [\#1151](https://github.com/bigdatagenomics/adam/issues/1151)
- vcf2adam unparsable vcf record [\#1149](https://github.com/bigdatagenomics/adam/issues/1149)
- Reorder kryo.register statements in ADAMKryoRegistrator [\#1146](https://github.com/bigdatagenomics/adam/issues/1146)
- Make region joins public again [\#1143](https://github.com/bigdatagenomics/adam/issues/1143)
- Support CRAM input/output [\#1141](https://github.com/bigdatagenomics/adam/issues/1141)
- Transform should run with spark.kryo.requireRegistration=true [\#1136](https://github.com/bigdatagenomics/adam/issues/1136)
- adam-shell not handling bash args correctly [\#1132](https://github.com/bigdatagenomics/adam/issues/1132)
- Remove Gene and related models and parsing code [\#1129](https://github.com/bigdatagenomics/adam/issues/1129)
- Generate Scoverage reports when running CI [\#1124](https://github.com/bigdatagenomics/adam/issues/1124)
- Remove PairingRDD [\#1122](https://github.com/bigdatagenomics/adam/issues/1122)
- SAMRecordConverter.convert takes unused arguments [\#1113](https://github.com/bigdatagenomics/adam/issues/1113)
- Add Pipe API [\#1112](https://github.com/bigdatagenomics/adam/issues/1112)
- Improve coverage in Feature unit tests [\#1106](https://github.com/bigdatagenomics/adam/issues/1106)
- K-mer.scala code [\#1105](https://github.com/bigdatagenomics/adam/issues/1105)
- add -single file output option to ADAM2Vcf [\#1102](https://github.com/bigdatagenomics/adam/issues/1102)
- adam2vcf Fails with Sample not serializable [\#1100](https://github.com/bigdatagenomics/adam/issues/1100)
- ReferenceRegion.apply(AlignmentRecord) should not NPE on unmapped reads [\#1099](https://github.com/bigdatagenomics/adam/issues/1099)
- Add outer region join implementations [\#1098](https://github.com/bigdatagenomics/adam/issues/1098)
- VariantContextConverter never returns DatabaseVariantAnnotation [\#1097](https://github.com/bigdatagenomics/adam/issues/1097)
- loadvcf: conflicting require statement [\#1094](https://github.com/bigdatagenomics/adam/issues/1094)
- ADAM version 0.19.0 will not run on Spark version 2.0.0 [\#1093](https://github.com/bigdatagenomics/adam/issues/1093)
- Be more rigorous with FileSystem.get [\#1087](https://github.com/bigdatagenomics/adam/issues/1087)
- Remove network-connected and default test-related Maven profiles [\#1073](https://github.com/bigdatagenomics/adam/issues/1073)
- Releases should get pushed to Spark Packages [\#1067](https://github.com/bigdatagenomics/adam/issues/1067)
- Invalid POM for cli on 0.19.0 [\#1066](https://github.com/bigdatagenomics/adam/issues/1066)
- scala.MatchError RegExp does not catch colons in value part properly [\#1061](https://github.com/bigdatagenomics/adam/issues/1061)
- Support writing IntervalList header for features [\#1059](https://github.com/bigdatagenomics/adam/issues/1059)
- Add -single support when writing features in native formats [\#1058](https://github.com/bigdatagenomics/adam/issues/1058)
- Remove workaround for gzip/BGZF compressed VCF headers [\#1057](https://github.com/bigdatagenomics/adam/issues/1057)
- Clean up if clauses in Transform [\#1053](https://github.com/bigdatagenomics/adam/issues/1053)
- Adam-0.18.2 can not load Adam-0.14.0 adamSave function data (sam) [\#1050](https://github.com/bigdatagenomics/adam/issues/1050)
- filterByOverlappingRegion Incorrect for Genotypes [\#1042](https://github.com/bigdatagenomics/adam/issues/1042)
- Move Interval trait to utils, added in #75 [\#1041](https://github.com/bigdatagenomics/adam/issues/1041)
- Remove implicit GenomicRDD to RDD conversion [\#1040](https://github.com/bigdatagenomics/adam/issues/1040)
- VCF sample metadata - proposal for a GenotypedSampleMetadata object [\#1039](https://github.com/bigdatagenomics/adam/issues/1039)
- [build system] ADAM test builds pollute /tmp, leaving lots of cruft... [\#1038](https://github.com/bigdatagenomics/adam/issues/1038)
- adamMarkDuplicates function in AlignmentRecordRDDFunctions class can not mark the same read? [\#1037](https://github.com/bigdatagenomics/adam/issues/1037)
- test MarkDuplicatesSuite with two similar read in ref and start position and different avgPhredScore, error! [\#1035](https://github.com/bigdatagenomics/adam/issues/1035)
- Explore protocol buffers vs Avro [\#1031](https://github.com/bigdatagenomics/adam/issues/1031)
- Increase Avro dependency version to 1.8.0 [\#1029](https://github.com/bigdatagenomics/adam/issues/1029)
- ADAM specific logging [\#1024](https://github.com/bigdatagenomics/adam/issues/1024)
- Reenable Travis CI for pull request builds [\#1023](https://github.com/bigdatagenomics/adam/issues/1023)
- Bump Apache Spark version to 1.6.1 in Jenkins [\#1022](https://github.com/bigdatagenomics/adam/issues/1022)
- ADAM compatibility with Spark 2.0 [\#1021](https://github.com/bigdatagenomics/adam/issues/1021)
- ADAM to BAM conversion failing on 1000G file [\#1013](https://github.com/bigdatagenomics/adam/issues/1013)
- Factor out *RDDFunctions classes [\#1011](https://github.com/bigdatagenomics/adam/issues/1011)
- Port single file BAM and header code to VCF [\#1009](https://github.com/bigdatagenomics/adam/issues/1009)
- Roll Jenkins JDK 8 changes into ./scripts/jenkins-test [\#1008](https://github.com/bigdatagenomics/adam/issues/1008)
- Support GFF3 format [\#1007](https://github.com/bigdatagenomics/adam/issues/1007)
- Separate fat jar build from adam-cli to new maven module [\#1006](https://github.com/bigdatagenomics/adam/issues/1006)
- adam-cli POM invalid: maven.build.timestamp [\#1004](https://github.com/bigdatagenomics/adam/issues/1004)
- Sub-partitioning of Parquet file for ADAM [\#1003](https://github.com/bigdatagenomics/adam/issues/1003)
- Flattening the Genotype schema [\#1002](https://github.com/bigdatagenomics/adam/issues/1002)
- install adam 0.19 error! [\#1001](https://github.com/bigdatagenomics/adam/issues/1001)
- How to solve it please? [\#1000](https://github.com/bigdatagenomics/adam/issues/1000)
- Has the project realized alignment reads to reference genome algorithm? [\#996](https://github.com/bigdatagenomics/adam/issues/996)
- All file-based input methods should support running on directories, compressed files, and wildcards [\#993](https://github.com/bigdatagenomics/adam/issues/993)
- Contig to ContigName Change not reflected in AlignmentRecordField [\#991](https://github.com/bigdatagenomics/adam/issues/991)
- Add homebrew guidelines to release checklist or automate PR generation [\#987](https://github.com/bigdatagenomics/adam/issues/987)
- fix deprecation warnings [\#985](https://github.com/bigdatagenomics/adam/issues/985)
- rename `fragments` package [\#984](https://github.com/bigdatagenomics/adam/issues/984)
- Explore if SeqDict data can be factored out more aggressively [\#983](https://github.com/bigdatagenomics/adam/issues/983)
- Make "Adam" all caps in filename Adam2Fastq.scala [\#981](https://github.com/bigdatagenomics/adam/issues/981)
- Adam2Fastq should output reverse complement when 0x10 flag is set for read [\#980](https://github.com/bigdatagenomics/adam/issues/980)
- Allow lowercase letters in jar/version names [\#974](https://github.com/bigdatagenomics/adam/issues/974)
- Add stringency parameter to flagstat [\#973](https://github.com/bigdatagenomics/adam/issues/973)
- Arg-array parsing problem in adam-submit [\#971](https://github.com/bigdatagenomics/adam/issues/971)
- Pass recordGroup parameter to loadPairedFastq [\#969](https://github.com/bigdatagenomics/adam/issues/969)
- Send a number of partitions to sc.textFile calls [\#968](https://github.com/bigdatagenomics/adam/issues/968)
- adamGetReferenceString doesn't reduce pairs correctly [\#967](https://github.com/bigdatagenomics/adam/issues/967)
- Update ADAM formula in homebrew-science to version 0.19.0 [\#963](https://github.com/bigdatagenomics/adam/issues/963)
- BAM output in ADAM appears to be corrupt [\#962](https://github.com/bigdatagenomics/adam/issues/962)
- Remove code workarounds necessary for Spark 1.2.1/Hadoop 1.0.x support [\#959](https://github.com/bigdatagenomics/adam/issues/959)
- Issue with version 18.0.2 [\#957](https://github.com/bigdatagenomics/adam/issues/957)
- Expose sorting by reference index [\#952](https://github.com/bigdatagenomics/adam/issues/952)
- .rgdict and .seqdict files are not placed in the adam directory [\#945](https://github.com/bigdatagenomics/adam/issues/945)
- Why does count_kmers not return k-mers that are split between two records? [\#930](https://github.com/bigdatagenomics/adam/issues/930)
- Load legacy file formats to Spark SQL Dataframes [\#912](https://github.com/bigdatagenomics/adam/issues/912)
- Clean up RDD method names [\#910](https://github.com/bigdatagenomics/adam/issues/910)
- Load/store sequence dictionaries alongside Genotype RDDs [\#909](https://github.com/bigdatagenomics/adam/issues/909)
- vcf2adam -print_metrics throws IllegalStateException on Spark 1.5.2 or later [\#902](https://github.com/bigdatagenomics/adam/issues/902)
- error: no reads in first split: bad BAM file or tiny split size? [\#896](https://github.com/bigdatagenomics/adam/issues/896)
- FastaConverter.FastaDescriptionLine not kryo-registered [\#893](https://github.com/bigdatagenomics/adam/issues/893)
- Work With ADAM fasta2adam in a distributed mode [\#881](https://github.com/bigdatagenomics/adam/issues/881)
- vcf2adam -> Exception in thread "main" java.lang.NoSuchMethodError: scala.Predef$.$conforms()Lscala/Predef$$less$colon$less; [\#871](https://github.com/bigdatagenomics/adam/issues/871)
- Code coverage profile is broken [\#849](https://github.com/bigdatagenomics/adam/issues/849)
- Building Adam on OS X 10.10.5 with Java 1.8 [\#835](https://github.com/bigdatagenomics/adam/issues/835)
- Normalize AlignmentRecord.recordGroup* fields onto a separate record type [\#828](https://github.com/bigdatagenomics/adam/issues/828)
- Gracefully handle missing Spark- and Hadoop-versions in jenkins-test; document how to set them. [\#827](https://github.com/bigdatagenomics/adam/issues/827)
- Use Adam File with Hive [\#820](https://github.com/bigdatagenomics/adam/issues/820)
- How do we handle reads that don't have original quality scores when converting to FASTQ with original qualities? [\#818](https://github.com/bigdatagenomics/adam/issues/818)
- SAMFileHeader "sort order" attribute being un-set during file-save job [\#800](https://github.com/bigdatagenomics/adam/issues/800)
- Use same sort order as Samtools [\#796](https://github.com/bigdatagenomics/adam/issues/796)
- RNAME and RNEXT fields jumbled on transform BAM->ADAM->BAM [\#795](https://github.com/bigdatagenomics/adam/issues/795)
- Support loading multiple indexed read files [\#787](https://github.com/bigdatagenomics/adam/issues/787)
- Duplicate OUTPUT command line argument metaVar in adam2fastq [\#776](https://github.com/bigdatagenomics/adam/issues/776)
- Allow Variant to ReferenceRegion conversion [\#768](https://github.com/bigdatagenomics/adam/issues/768)
- Spark Errors References Deprecated SPARK_CLASSPATH [\#767](https://github.com/bigdatagenomics/adam/issues/767)
- Spark Errors References Deprecated SPARK_CLASSPATH [\#766](https://github.com/bigdatagenomics/adam/issues/766)
- adam2vcf fails with -coalesce [\#735](https://github.com/bigdatagenomics/adam/issues/735)
- Writing to a BAM file with adamSAMSave consistently fails [\#721](https://github.com/bigdatagenomics/adam/issues/721)
- BQSR on C835.HCC1143_BL.4 uses excessive amount of driver memory [\#714](https://github.com/bigdatagenomics/adam/issues/714)
- Support writing RDD[Feature] to various file formats [\#710](https://github.com/bigdatagenomics/adam/issues/710)
- adamParquetSave has a menacing false error message about *.adam extension [\#681](https://github.com/bigdatagenomics/adam/issues/681)
- BAMHeader not set when running on a cluster [\#676](https://github.com/bigdatagenomics/adam/issues/676)
- spark 1.3.1 upgarde to hortonworks HDP 2.2.4.2-2? [\#675](https://github.com/bigdatagenomics/adam/issues/675)
- `Symbol` case class is nucleotide-centric [\#672](https://github.com/bigdatagenomics/adam/issues/672)
- xAssembler cannot be build using mvn [\#658](https://github.com/bigdatagenomics/adam/issues/658)
- adam-submit VerifyError [\#642](https://github.com/bigdatagenomics/adam/issues/642)
- vcf2adam : Unsupported type ENUM [\#638](https://github.com/bigdatagenomics/adam/issues/638)
- Update CDH documentation [\#615](https://github.com/bigdatagenomics/adam/issues/615)
- Remove and generalize plugin code [\#602](https://github.com/bigdatagenomics/adam/issues/602)
- Fix record oriented shuffle [\#599](https://github.com/bigdatagenomics/adam/issues/599)
- Migrate preprocessing stages out of ADAM [\#598](https://github.com/bigdatagenomics/adam/issues/598)
- Publish/socialize a roadmap [\#591](https://github.com/bigdatagenomics/adam/issues/591)
- Eliminate format detection and extension checks for loading data [\#587](https://github.com/bigdatagenomics/adam/issues/587)
- Improve error message when we can't find a ReferenceRegion for a contig [\#582](https://github.com/bigdatagenomics/adam/issues/582)
- Do reference partitioners restrict a partition to contain keys from a single contig? [\#573](https://github.com/bigdatagenomics/adam/issues/573)
- Connection refused errors when transforming BAM file with BQSR [\#516](https://github.com/bigdatagenomics/adam/issues/516)
- ReferenceRegion shouldn't extend Ordered [\#511](https://github.com/bigdatagenomics/adam/issues/511)
- Documentation for common usecases [\#491](https://github.com/bigdatagenomics/adam/issues/491)
- Improve handling of "*" sequences during BQSR [\#484](https://github.com/bigdatagenomics/adam/issues/484)
- Original qualities are parsed out, but left in attribute fields [\#483](https://github.com/bigdatagenomics/adam/issues/483)
- Need a FileLocator that mirrors the use of Path in HDFS [\#477](https://github.com/bigdatagenomics/adam/issues/477)
- FileLocator should support finding "child" locators. [\#476](https://github.com/bigdatagenomics/adam/issues/476)
- Add S3 based Parquet directory loader [\#463](https://github.com/bigdatagenomics/adam/issues/463)
- Should FASTQ output use reads' "original qualities"? [\#436](https://github.com/bigdatagenomics/adam/issues/436)
- VcfStringUtils unused? [\#428](https://github.com/bigdatagenomics/adam/issues/428)
- We should be able to filter genotypes that overlap a region [\#422](https://github.com/bigdatagenomics/adam/issues/422)
- Create a simplified vocabulary for naming projections. [\#419](https://github.com/bigdatagenomics/adam/issues/419)
- Update documentation [\#406](https://github.com/bigdatagenomics/adam/issues/406)
- Bake off different region join implementations [\#395](https://github.com/bigdatagenomics/adam/issues/395)
- Handle no-ops more intelligently when creating MD tags [\#392](https://github.com/bigdatagenomics/adam/issues/392)
- Remove all the commands in the "CONVERSION OPERATIONS" `CommandGroup` [\#373](https://github.com/bigdatagenomics/adam/issues/373)
- Fail to Write RDD into HDFS with Parquet Format [\#344](https://github.com/bigdatagenomics/adam/issues/344)
- Refactor ReferencePositionWithOrientation [\#317](https://github.com/bigdatagenomics/adam/issues/317)
- Add docs about SPARK_LOCAL_IP [\#305](https://github.com/bigdatagenomics/adam/issues/305)
- PartitionAndJoin should throw an exception if it sees an unmapped read [\#297](https://github.com/bigdatagenomics/adam/issues/297)
- Add insert size calculation [\#296](https://github.com/bigdatagenomics/adam/issues/296)
- Newbie questions - learning resources? Reading a range of records from Adam? [\#281](https://github.com/bigdatagenomics/adam/issues/281)
- Add variant effect ontology [\#261](https://github.com/bigdatagenomics/adam/issues/261)
- Don't flatten optional SAM tags into a string [\#240](https://github.com/bigdatagenomics/adam/issues/240)
- Characterize impact of partition size on pileup creation [\#163](https://github.com/bigdatagenomics/adam/issues/163)
- Need to support BCF output format [\#153](https://github.com/bigdatagenomics/adam/issues/153)
- Allow list of commands to be injected into adam-cli AdamMain [\#132](https://github.com/bigdatagenomics/adam/issues/132)
- Parse out common annotations stored in VCF format [\#118](https://github.com/bigdatagenomics/adam/issues/118)
- Update normalization code to enable normalization of sequences with more than two indels [\#64](https://github.com/bigdatagenomics/adam/issues/64)
- Add clipping heuristic to indel realigner [\#63](https://github.com/bigdatagenomics/adam/issues/63)
- BQSR should support recalibration across multiple ADAM files [\#58](https://github.com/bigdatagenomics/adam/issues/58)

**Merged and closed pull requests:**

- fix SB tag parsing [\#1209](https://github.com/bigdatagenomics/adam/pull/1209) ([fnothaft](https://github.com/fnothaft))
- Fastq record converter [\#1208](https://github.com/bigdatagenomics/adam/pull/1208) ([fnothaft](https://github.com/fnothaft))
- Doc suggested partitionSize in ShuffleRegionJoin [\#1207](https://github.com/bigdatagenomics/adam/pull/1207) ([jpdna](https://github.com/jpdna))
- Test demonstrating region join failure [\#1206](https://github.com/bigdatagenomics/adam/pull/1206) ([jpdna](https://github.com/jpdna))
- fix SB tag parsing [\#1203](https://github.com/bigdatagenomics/adam/pull/1203) ([jpdna](https://github.com/jpdna))
- fix build [\#1201](https://github.com/bigdatagenomics/adam/pull/1201) ([ryan-williams](https://github.com/ryan-williams))
- \[ADAM-1192\] Correctly handle other whitespace in FASTA description. [\#1198](https://github.com/bigdatagenomics/adam/pull/1198) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1190\] Manually (un)pack IndelRealignmentTarget set. [\#1191](https://github.com/bigdatagenomics/adam/pull/1191) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1188\] Delete scripts/commit-pr.sh [\#1189](https://github.com/bigdatagenomics/adam/pull/1189) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1186\] Mask null from fs.globStatus. [\#1187](https://github.com/bigdatagenomics/adam/pull/1187) ([fnothaft](https://github.com/fnothaft))
- Fastq record converter [\#1185](https://github.com/bigdatagenomics/adam/pull/1185) ([zyxue](https://github.com/zyxue))
- \[ADAM-1182\] isSorted=true should write SO:coordinate in SAM/BAM/CRAM header. [\#1183](https://github.com/bigdatagenomics/adam/pull/1183) ([fnothaft](https://github.com/fnothaft))
- Add scoverage aggregator and fail on low coverage. [\#1181](https://github.com/bigdatagenomics/adam/pull/1181) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1179\] Improve error message when globbing a parquet file fails. [\#1180](https://github.com/bigdatagenomics/adam/pull/1180) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1176\] Update command line doc and examples in README.md [\#1177](https://github.com/bigdatagenomics/adam/pull/1177) ([heuermh](https://github.com/heuermh))
- Refactor CLIs for merging sharded files [\#1167](https://github.com/bigdatagenomics/adam/pull/1167) ([fnothaft](https://github.com/fnothaft))
- Update Hadoop-BAM to version 7.7.0 [\#1166](https://github.com/bigdatagenomics/adam/pull/1166) ([heuermh](https://github.com/heuermh))
- \[ADAM-1162\] Write record group string name. [\#1163](https://github.com/bigdatagenomics/adam/pull/1163) ([fnothaft](https://github.com/fnothaft))
- Map IntervalList format column four to feature name [\#1159](https://github.com/bigdatagenomics/adam/pull/1159) ([heuermh](https://github.com/heuermh))
- Make AlignmentRecordConverter public so that it can be used from other projects [\#1157](https://github.com/bigdatagenomics/adam/pull/1157) ([tomwhite](https://github.com/tomwhite))
- added predicate option to loadCoverage [\#1156](https://github.com/bigdatagenomics/adam/pull/1156) ([akmorrow13](https://github.com/akmorrow13))
- \[ADAM-1154\] Change set -x to set -e in ./bin/adam-shell. [\#1155](https://github.com/bigdatagenomics/adam/pull/1155) ([fnothaft](https://github.com/fnothaft))
- Remove Gene and related models and parsing code [\#1153](https://github.com/bigdatagenomics/adam/pull/1153) ([heuermh](https://github.com/heuermh))
- Reorder kryo.register statements in ADAMKryoRegistrator [\#1148](https://github.com/bigdatagenomics/adam/pull/1148) ([heuermh](https://github.com/heuermh))
- Updated GenomicPartitioners to accept additional key. [\#1147](https://github.com/bigdatagenomics/adam/pull/1147) ([akmorrow13](https://github.com/akmorrow13))
- \[ADAM-1141\] Add support for saving/loading AlignmentRecords to/from CRAM. [\#1145](https://github.com/bigdatagenomics/adam/pull/1145) ([fnothaft](https://github.com/fnothaft))
- misc pom/test/resource improvements [\#1142](https://github.com/bigdatagenomics/adam/pull/1142) ([ryan-williams](https://github.com/ryan-williams))
- \[ADAM-1136\] Transform runs successfully with kryo registration required [\#1138](https://github.com/bigdatagenomics/adam/pull/1138) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1132\] Fix improper quoting of bash args in adam-shell. [\#1133](https://github.com/bigdatagenomics/adam/pull/1133) ([fnothaft](https://github.com/fnothaft))
- Remove StructuralVariant and StructuralVariantType, add names field to Variant [\#1131](https://github.com/bigdatagenomics/adam/pull/1131) ([heuermh](https://github.com/heuermh))
- Remove StructuralVariant and StructuralVariantType, add names field to Variant [\#1130](https://github.com/bigdatagenomics/adam/pull/1130) ([heuermh](https://github.com/heuermh))
- PR #1108 with issue #1122 [\#1128](https://github.com/bigdatagenomics/adam/pull/1128) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1038\] Eliminate writing to /tmp during CI builds. [\#1127](https://github.com/bigdatagenomics/adam/pull/1127) ([fnothaft](https://github.com/fnothaft))
- Update for bdg-formats code style changes [\#1126](https://github.com/bigdatagenomics/adam/pull/1126) ([heuermh](https://github.com/heuermh))
- \[ADAM-1124\] Add Scoverage and generate coverage reports in Jenkins. [\#1125](https://github.com/bigdatagenomics/adam/pull/1125) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1093\] Move to support Spark 2.0.0. [\#1123](https://github.com/bigdatagenomics/adam/pull/1123) ([fnothaft](https://github.com/fnothaft))
- remove duplicated dependency [\#1119](https://github.com/bigdatagenomics/adam/pull/1119) ([ryan-williams](https://github.com/ryan-williams))
- Clean up ADAMContext [\#1118](https://github.com/bigdatagenomics/adam/pull/1118) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-993\] Support loading files using globs and from directory paths. [\#1117](https://github.com/bigdatagenomics/adam/pull/1117) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1087\] Migrate away from FileSystem.get [\#1116](https://github.com/bigdatagenomics/adam/pull/1116) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1099\] Make reference region not throw NPE. [\#1115](https://github.com/bigdatagenomics/adam/pull/1115) ([fnothaft](https://github.com/fnothaft))
- Add pipes API [\#1114](https://github.com/bigdatagenomics/adam/pull/1114) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1105\] Use assembly jar in adam-shell. [\#1111](https://github.com/bigdatagenomics/adam/pull/1111) ([fnothaft](https://github.com/fnothaft))
- Add outer joins [\#1109](https://github.com/bigdatagenomics/adam/pull/1109) ([fnothaft](https://github.com/fnothaft))
- Modified CalculateDepth to calcuate coverage from alignment files [\#1108](https://github.com/bigdatagenomics/adam/pull/1108) ([akmorrow13](https://github.com/akmorrow13))
- Resolves various single file save/header issues [\#1104](https://github.com/bigdatagenomics/adam/pull/1104) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1100\] Resolve Sample Not Serializable exception [\#1101](https://github.com/bigdatagenomics/adam/pull/1101) ([fnothaft](https://github.com/fnothaft))
- added loadIndexedVcf and loadIndexedBam for multiple ReferenceRegions [\#1096](https://github.com/bigdatagenomics/adam/pull/1096) ([akmorrow13](https://github.com/akmorrow13))
- Added support for Indexed VCF files [\#1095](https://github.com/bigdatagenomics/adam/pull/1095) ([akmorrow13](https://github.com/akmorrow13))
- \[ADAM-582\] Eliminate .get on option in FragmentCoverter. [\#1091](https://github.com/bigdatagenomics/adam/pull/1091) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-776\] Rename duplicate OUTPUT metaVar in ADAM2Fastq. [\#1090](https://github.com/bigdatagenomics/adam/pull/1090) ([fnothaft](https://github.com/fnothaft))
- refactored ReferenceFile to require SequenceDictionary [\#1086](https://github.com/bigdatagenomics/adam/pull/1086) ([akmorrow13](https://github.com/akmorrow13))
- \[ADAM-1073\] Remove network-connected and default test-related Maven profiles [\#1082](https://github.com/bigdatagenomics/adam/pull/1082) ([heuermh](https://github.com/heuermh))
- \[ADAM-1053\] Clean up Transform [\#1081](https://github.com/bigdatagenomics/adam/pull/1081) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1061\] Clean up attributes regex and denormalized fields [\#1080](https://github.com/bigdatagenomics/adam/pull/1080) ([fnothaft](https://github.com/fnothaft))
- Extended TwoBitFile and NucleotideContigFragmentRDDFunctions to behave more similar [\#1079](https://github.com/bigdatagenomics/adam/pull/1079) ([akmorrow13](https://github.com/akmorrow13))
- Refactor variant and genotype annotations [\#1078](https://github.com/bigdatagenomics/adam/pull/1078) ([heuermh](https://github.com/heuermh))
- \[ADAM-1039\] Add basic support for Sample record. [\#1077](https://github.com/bigdatagenomics/adam/pull/1077) ([fnothaft](https://github.com/fnothaft))
- Remove code workarounds necessary for Spark 1.2.1/Hadoop 1.0.x support [\#1076](https://github.com/bigdatagenomics/adam/pull/1076) ([heuermh](https://github.com/heuermh))
- \[ADAM-194\] Use separate filtersFailed and filtersPassed arrays for variant quality filters [\#1075](https://github.com/bigdatagenomics/adam/pull/1075) ([heuermh](https://github.com/heuermh))
- Whitespace code style fixes [\#1074](https://github.com/bigdatagenomics/adam/pull/1074) ([heuermh](https://github.com/heuermh))
- \[ADAM-1006\] Split überjar out to adam-assembly submodule. [\#1072](https://github.com/bigdatagenomics/adam/pull/1072) ([fnothaft](https://github.com/fnothaft))
- Remove code coverage profile [\#1071](https://github.com/bigdatagenomics/adam/pull/1071) ([heuermh](https://github.com/heuermh))
- \[ADAM-768\] ReferenceRegion from variant/genotypes [\#1070](https://github.com/bigdatagenomics/adam/pull/1070) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1044\] Support VCF annotation ANN field [\#1069](https://github.com/bigdatagenomics/adam/pull/1069) ([heuermh](https://github.com/heuermh))
- \[ADAM-1067\] Add release documentation and scripting for Spark Packages. [\#1068](https://github.com/bigdatagenomics/adam/pull/1068) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-602\] Remove plugin code. [\#1065](https://github.com/bigdatagenomics/adam/pull/1065) ([fnothaft](https://github.com/fnothaft))
- Refactoring `org.bdgenomics.adam.io` package. [\#1064](https://github.com/bigdatagenomics/adam/pull/1064) ([fnothaft](https://github.com/fnothaft))
- Cleanup in org.bdgenomics.adam.converters package. [\#1062](https://github.com/bigdatagenomics/adam/pull/1062) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1057\] Remove workaround for gzip/BGZF compressed VCF headers [\#1057](https://github.com/bigdatagenomics/adam/pull/1057) ([heuermh](https://github.com/heuermh))
- Cleanup on `org.bdgenomics.adam.algorithms.smithwaterman` package. [\#1056](https://github.com/bigdatagenomics/adam/pull/1056) ([fnothaft](https://github.com/fnothaft))
- Documentation cleanup and minor refactor on the consensus package. [\#1055](https://github.com/bigdatagenomics/adam/pull/1055) ([fnothaft](https://github.com/fnothaft))
- Add KEYS with public code signing keys [\#1054](https://github.com/bigdatagenomics/adam/pull/1054) ([heuermh](https://github.com/heuermh))
- Adding GA4GH 0.5.1 converter for reads. [\#1052](https://github.com/bigdatagenomics/adam/pull/1052) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-1011\] Refactor to add GenomicRDDs for all Avro types [\#1051](https://github.com/bigdatagenomics/adam/pull/1051) ([fnothaft](https://github.com/fnothaft))
- removed interval trait and redirected to interval in utils-intervalrdd [\#1046](https://github.com/bigdatagenomics/adam/pull/1046) ([akmorrow13](https://github.com/akmorrow13))
- \[ADAM-952\] Expose sorting by reference index. [\#1045](https://github.com/bigdatagenomics/adam/pull/1045) ([fnothaft](https://github.com/fnothaft))
- overlap query reflects new formats [\#1043](https://github.com/bigdatagenomics/adam/pull/1043) ([erictu](https://github.com/erictu))
- Changed loadIndexedBam to use hadoop-bam InputFormat [\#1036](https://github.com/bigdatagenomics/adam/pull/1036) ([fnothaft](https://github.com/fnothaft))
- Increase Avro dependency version to 1.8.0 [\#1034](https://github.com/bigdatagenomics/adam/pull/1034) ([heuermh](https://github.com/heuermh))
- Improved README fix using feedback from other approach review. [\#1034](https://github.com/bigdatagenomics/adam/pull/1034) ([InvisibleTech](https://github.com/InvisibleTech))
- Error in the README.md for kmer.scala example, need to get rdd first. [\#1032](https://github.com/bigdatagenomics/adam/pull/1032) ([InvisibleTech](https://github.com/InvisibleTech))
- Add fragmentEndPosition to NucleotideContigFragment [\#1030](https://github.com/bigdatagenomics/adam/pull/1030) ([heuermh](https://github.com/heuermh))
- Logging to be done by ADAM utils code rather than Spark [\#1028](https://github.com/bigdatagenomics/adam/pull/1028) ([jpdna](https://github.com/jpdna))
- add maxScore [\#1027](https://github.com/bigdatagenomics/adam/pull/1027) ([xubo245](https://github.com/xubo245))
- \[ADAM-1008\] Modify jenkins-test script to support Java 8 build. [\#1026](https://github.com/bigdatagenomics/adam/pull/1026) ([fnothaft](https://github.com/fnothaft))
- whitespace change, do not merge [\#1025](https://github.com/bigdatagenomics/adam/pull/1025) ([shaneknapp](https://github.com/shaneknapp))
- require kryo registration in tests [\#1020](https://github.com/bigdatagenomics/adam/pull/1020) ([ryan-williams](https://github.com/ryan-williams))
- print full stack traces on test failures [\#1019](https://github.com/bigdatagenomics/adam/pull/1019) ([ryan-williams](https://github.com/ryan-williams))
- bump commons-io version [\#1017](https://github.com/bigdatagenomics/adam/pull/1017) ([ryan-williams](https://github.com/ryan-williams))
- exclude javadoc jar in adam-shell [\#1016](https://github.com/bigdatagenomics/adam/pull/1016) ([ryan-williams](https://github.com/ryan-williams))
- \[ADAM-909\] Refactoring variation RDDs. [\#1015](https://github.com/bigdatagenomics/adam/pull/1015) ([fnothaft](https://github.com/fnothaft))
- Modified CalculateDepth to get coverage on whole alignment adam files [\#1010](https://github.com/bigdatagenomics/adam/pull/1010) ([akmorrow13](https://github.com/akmorrow13))
- \[ADAM-1004\] Remove recursive maven.build.timestamp declaration [\#1005](https://github.com/bigdatagenomics/adam/pull/1005) ([heuermh](https://github.com/heuermh))
- Maint 2.11 0.19.0 [\#999](https://github.com/bigdatagenomics/adam/pull/999) ([tushu1232](https://github.com/tushu1232))
- \[ADAM-710\] Add saveAs methods for feature formats GTF, BED, IntervalList, and NarrowPeak [\#998](https://github.com/bigdatagenomics/adam/pull/998) ([heuermh](https://github.com/heuermh))
- Moving Adam2Fastq to ADAM2Fastq [\#995](https://github.com/bigdatagenomics/adam/pull/995) ([heuermh](https://github.com/heuermh))
- Update release doc for CHANGES.md and homebrew [\#994](https://github.com/bigdatagenomics/adam/pull/994) ([heuermh](https://github.com/heuermh))
- Update to AlignmentRecordField and its usages as contig changed to co… [\#992](https://github.com/bigdatagenomics/adam/pull/992) ([jpdna](https://github.com/jpdna))
- \[ADAM-974\] Short term fix for multiple ADAM cli assembly jars check [\#990](https://github.com/bigdatagenomics/adam/pull/990) ([heuermh](https://github.com/heuermh))
- Update hadoop-bam dependency version to 7.5.0 [\#989](https://github.com/bigdatagenomics/adam/pull/989) ([heuermh](https://github.com/heuermh))
- Replaced Contig with ContigName in AlignmentRecord and related changes [\#988](https://github.com/bigdatagenomics/adam/pull/988) ([jpdna](https://github.com/jpdna))
- fix some deprecation/style things and rename a pkg [\#986](https://github.com/bigdatagenomics/adam/pull/986) ([ryan-williams](https://github.com/ryan-williams))
- Fix Adam2fastq in case of read with both reverse and unmapped flags [\#982](https://github.com/bigdatagenomics/adam/pull/982) ([jpdna](https://github.com/jpdna))
- \[ADAM-510\] Refactoring RDD function names [\#979](https://github.com/bigdatagenomics/adam/pull/979) ([heuermh](https://github.com/heuermh))
- Use .adam/_{seq,rg}dict.avro paths for Avro-formatted dictionaries [\#978](https://github.com/bigdatagenomics/adam/pull/978) ([heuermh](https://github.com/heuermh))
- Remove unused file VcfHeaderUtils.scala [\#977](https://github.com/bigdatagenomics/adam/pull/977) ([heuermh](https://github.com/heuermh))
- add validation stringency to bam parsing, flagstat [\#976](https://github.com/bigdatagenomics/adam/pull/976) ([ryan-williams](https://github.com/ryan-williams))
- more permissible jar regex in adam-submit [\#975](https://github.com/bigdatagenomics/adam/pull/975) ([ryan-williams](https://github.com/ryan-williams))
- fix bash arg array processing in adam-submit [\#972](https://github.com/bigdatagenomics/adam/pull/972) ([ryan-williams](https://github.com/ryan-williams))
- adamGetReferenceString reduces pairs correctly, fixes #967 [\#970](https://github.com/bigdatagenomics/adam/pull/970) ([erictu](https://github.com/erictu))
- A few improvements [\#966](https://github.com/bigdatagenomics/adam/pull/966) ([ryan-williams](https://github.com/ryan-williams))
- improve SW performance by replacing functional reductions with imperative ones [\#965](https://github.com/bigdatagenomics/adam/pull/965) ([noamBarkai](https://github.com/noamBarkai))
- \[ADAM-962\] Fix corrupt single-file BAM output. [\#964](https://github.com/bigdatagenomics/adam/pull/964) ([fnothaft](https://github.com/fnothaft))
- \[ADAM-960\] Updating bdg-utils dependency version to 0.2.4 [\#961](https://github.com/bigdatagenomics/adam/pull/961) ([heuermh](https://github.com/heuermh))
- \[ADAM-946\] Fixes to FlagStat for Samtools concordance issue [\#954](https://github.com/bigdatagenomics/adam/pull/954) ([jpdna](https://github.com/jpdna))
- Use hadoop-bam BAMInputFormat to do loadIndexedBam [\#953](https://github.com/bigdatagenomics/adam/pull/953) ([andrewmchen](https://github.com/andrewmchen))
- Add -print_metrics option to Jenkins build [\#947](https://github.com/bigdatagenomics/adam/pull/947) ([heuermh](https://github.com/heuermh))
- adam2vcf doesn't have info fields [\#939](https://github.com/bigdatagenomics/adam/pull/939) ([andrewmchen](https://github.com/andrewmchen))
- \[ADAM-893\] Register missing serializers. [\#933](https://github.com/bigdatagenomics/adam/pull/933) ([fnothaft](https://github.com/fnothaft))

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
