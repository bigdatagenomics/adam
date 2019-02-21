# ADAM Changelog #

### Version 0.26.0 ###

**Closed issues:**

 - Bump Spark dependency to version 2.3.3 [\#2120](https://github.com/bigdatagenomics/adam/issues/2120)
 - Update Spark version on Jenkins to 2.2.3 [\#2115](https://github.com/bigdatagenomics/adam/issues/2115)
 - Inverted duplicates are not found in mark duplicates [\#2102](https://github.com/bigdatagenomics/adam/issues/2102)
 - Py4JError: org.bdgenomics.adam.algorithms.consensus.ConsensusGenerator.fromKnowns does not exist in the JVM [\#2099](https://github.com/bigdatagenomics/adam/issues/2099)
 - Update Bioconda recipe for ADAM 0.25.0 [\#2088](https://github.com/bigdatagenomics/adam/issues/2088)
 - Update Homebrew formula for ADAM 0.25.0 [\#2087](https://github.com/bigdatagenomics/adam/issues/2087)
 - Error: Dependency package(s) 'SparkR' not available [\#2086](https://github.com/bigdatagenomics/adam/issues/2086)
 - Java-friendly indel realignment method doesn't allow passing reference [\#2013](https://github.com/bigdatagenomics/adam/issues/2013)
 - Use consistent (Scala-specific) (Java-specific) qualifiers in method scaladoc [\#1986](https://github.com/bigdatagenomics/adam/issues/1986)
 - Clarify GenomicRDD vs. GenomicDataset name [\#1954](https://github.com/bigdatagenomics/adam/issues/1954)
 - Support validation stringency in out formatters [\#1949](https://github.com/bigdatagenomics/adam/issues/1949)
 - Compute coverage by sample [\#1498](https://github.com/bigdatagenomics/adam/issues/1498)

**Merged and closed pull requests:**

 - Bump bdg-formats dependency to version 0.12.0. [\#2124](https://github.com/bigdatagenomics/adam/pull/2124) ([heuermh](https://github.com/heuermh))
 - [ADAM-2120] Bump Spark dependency to version 2.3.3. [\#2121](https://github.com/bigdatagenomics/adam/pull/2121) ([heuermh](https://github.com/heuermh))
 - Filter supplemental reads from scoring [\#2119](https://github.com/bigdatagenomics/adam/pull/2119) ([pauldwolfe](https://github.com/pauldwolfe))
 - [ADAM-2115] Update Spark version on Jenkins to 2.2.3. [\#2118](https://github.com/bigdatagenomics/adam/pull/2118) ([heuermh](https://github.com/heuermh))
 -  Refactor AlignmentRecord, RecordGroup, and ProcessingStep [\#2113](https://github.com/bigdatagenomics/adam/pull/2113) ([heuermh](https://github.com/heuermh))
 - removed anaconda requirement for venv during jenkins test [\#2109](https://github.com/bigdatagenomics/adam/pull/2109) ([akmorrow13](https://github.com/akmorrow13))
 - Propagate read negative flag to SAM records for unmapped reads [\#2105](https://github.com/bigdatagenomics/adam/pull/2105) ([henrydavidge](https://github.com/henrydavidge))
 - Add consensus targets to realignment targets [\#2104](https://github.com/bigdatagenomics/adam/pull/2104) ([pauldwolfe](https://github.com/pauldwolfe))
 - [ADAM-2099] Add python realignIndelsFromKnownIndels method [\#2103](https://github.com/bigdatagenomics/adam/pull/2103) ([heuermh](https://github.com/heuermh))
 - [ADAM-2102] Inverted duplicates are not found in mark duplicates [\#2101](https://github.com/bigdatagenomics/adam/pull/2101) ([pauldwolfe](https://github.com/pauldwolfe))
 - Rename contig to reference [\#2100](https://github.com/bigdatagenomics/adam/pull/2100) ([heuermh](https://github.com/heuermh))
 - [ADAM-1986] Add java-specific methods where missing. [\#2097](https://github.com/bigdatagenomics/adam/pull/2097) ([heuermh](https://github.com/heuermh))
 - [ADAM-2013] Add java-friendly indel realignment method that accepts reference. [\#2095](https://github.com/bigdatagenomics/adam/pull/2095) ([heuermh](https://github.com/heuermh))
 - Use build-helper-maven-plugin for build timestamp [\#2093](https://github.com/bigdatagenomics/adam/pull/2093) ([heuermh](https://github.com/heuermh))
 - bump adam-python version to 0.25.0a0 [\#2092](https://github.com/bigdatagenomics/adam/pull/2092) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-2085] Update R installation docs re: libgit2 and SparkR. [\#2090](https://github.com/bigdatagenomics/adam/pull/2090) ([heuermh](https://github.com/heuermh))
 - [ADAM-1954] Complete refactoring GenomicRDD to GenomicDataset. [\#1981](https://github.com/bigdatagenomics/adam/pull/1981) ([heuermh](https://github.com/heuermh))
 - [ADAM-1949] Support validation stringency in out formatters. [\#1969](https://github.com/bigdatagenomics/adam/pull/1969) ([heuermh](https://github.com/heuermh))


### Version 0.25.0 ###

**Closed issues:**

 - Expand illumina metadata regex to include "N" character  [\#2079](https://github.com/bigdatagenomics/adam/issues/2079)
 - Remove support for Hadoop 2.6 [\#2073](https://github.com/bigdatagenomics/adam/issues/2073)
 - NumberFormatException: For input string: "nan" in VCF [\#2068](https://github.com/bigdatagenomics/adam/issues/2068)
 - Support Spark 2.3.2 [\#2062](https://github.com/bigdatagenomics/adam/issues/2062)
 - Arrays should be passed to HTSJDK in the JVM primitive type [\#2059](https://github.com/bigdatagenomics/adam/issues/2059)
 - toCoverage() function for alignments does not distinguish samples [\#2049](https://github.com/bigdatagenomics/adam/issues/2049)
 - Building from adam-core module directory fails to generate Scala code for sql package [\#2047](https://github.com/bigdatagenomics/adam/issues/2047)
 - Data Sets [\#2043](https://github.com/bigdatagenomics/adam/issues/2043)
 - saveAsBed writes missing score values as '.' instead of '0' [\#2039](https://github.com/bigdatagenomics/adam/issues/2039)
 - Fix GFF3 parser to handle trailing FASTA [\#2037](https://github.com/bigdatagenomics/adam/issues/2037)
 - Add StorageLevel as an optional parameter to loadPairedFastq [\#2032](https://github.com/bigdatagenomics/adam/issues/2032)
 - Error: File name too long when building on encrypted file system [\#2031](https://github.com/bigdatagenomics/adam/issues/2031)
 - Fail to transform a VCF  file containing multiple genome data (Muliple sample) [\#2029](https://github.com/bigdatagenomics/adam/issues/2029)
 - Dataset and RDD constructors are missing from CoverageRDD [\#2027](https://github.com/bigdatagenomics/adam/issues/2027)
 - How to create a single RDD[Genotype] object out of multiple VCF files? [\#2025](https://github.com/bigdatagenomics/adam/issues/2025)
 - ReadTheDocs github banner is broken [\#2020](https://github.com/bigdatagenomics/adam/issues/2020)
 - -realign_indels throws serialization error with instrumentation enabled [\#2007](https://github.com/bigdatagenomics/adam/issues/2007)
 - Support 0 length FASTQ reads [\#2006](https://github.com/bigdatagenomics/adam/issues/2006)
 - Speed of Reading into ADAM RDDs from S3 [\#2003](https://github.com/bigdatagenomics/adam/issues/2003)
 - Support Python 3 [\#1999](https://github.com/bigdatagenomics/adam/issues/1999)
 - Unordered list of region join types in doc is missing nested levels [\#1997](https://github.com/bigdatagenomics/adam/issues/1997)
 - Add VariantContextRDD.saveAsPartitionedParquet, ADAMContext.loadPartitionedParquetVariantContexts [\#1996](https://github.com/bigdatagenomics/adam/issues/1996)
 - VCF annotation question [\#1994](https://github.com/bigdatagenomics/adam/issues/1994)
 - Fastq reader clips long reads at 10,000 bp [\#1992](https://github.com/bigdatagenomics/adam/issues/1992)
 - adam-submit Error: Number of executors must be a positive number on EMR 5.13.0/Spark 2.3.0 [\#1991](https://github.com/bigdatagenomics/adam/issues/1991)
 - Test against Spark 2.3.1, Parquet 1.8.3 [\#1989](https://github.com/bigdatagenomics/adam/issues/1989)
 - END does not get set when writing a gVCF [\#1988](https://github.com/bigdatagenomics/adam/issues/1988)
 - Support saving single files to filesystems that don't implement getScheme [\#1984](https://github.com/bigdatagenomics/adam/issues/1984)
 - Add additional filter by convenience methods [\#1978](https://github.com/bigdatagenomics/adam/issues/1978)
 - Limiting FragmentRDD pipe paralellism [\#1977](https://github.com/bigdatagenomics/adam/issues/1977)
 - Consider javadoc.io for API documentation linking [\#1976](https://github.com/bigdatagenomics/adam/issues/1976)
 - FASTQ Reader leaks connections [\#1974](https://github.com/bigdatagenomics/adam/issues/1974)
 - Update bioconda recipe for version 0.24.0 [\#1971](https://github.com/bigdatagenomics/adam/issues/1971)
 - Update homebrew formula at brewsci/homebrew-bio for version 0.24.0 [\#1970](https://github.com/bigdatagenomics/adam/issues/1970)
 - loadPartitionedParquetAlignments fails with Reference.all [\#1967](https://github.com/bigdatagenomics/adam/issues/1967)
 - Caused by: java.lang.VerifyError: class com.fasterxml.jackson.module.scala.ser.ScalaIteratorSerializer overrides final method withResolved [\#1953](https://github.com/bigdatagenomics/adam/issues/1953)
 - FASTQ input format needs to support index sequences [\#1697](https://github.com/bigdatagenomics/adam/issues/1697)
 - Changelog must be edited and committed manually during release process [\#936](https://github.com/bigdatagenomics/adam/issues/936)

**Merged and closed pull requests:**

 - added pyspark mock modules for API documentation [\#2084](https://github.com/bigdatagenomics/adam/pull/2084) ([akmorrow13](https://github.com/akmorrow13))
 - Added mock python modules for API python documentation [\#2082](https://github.com/bigdatagenomics/adam/pull/2082) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-2079] Expand illumina metadata regex to include "N" character [\#2081](https://github.com/bigdatagenomics/adam/pull/2081) ([pauldwolfe](https://github.com/pauldwolfe))
 - ADAM-2079 Added "N" to regexs for illumina metadata [\#2080](https://github.com/bigdatagenomics/adam/pull/2080) ([pauldwolfe](https://github.com/pauldwolfe))
 - Update docs with new template and documentation [\#2078](https://github.com/bigdatagenomics/adam/pull/2078) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1992] Make maximum FASTQ read length configurable. [\#2077](https://github.com/bigdatagenomics/adam/pull/2077) ([heuermh](https://github.com/heuermh))
 -  [ADAM-2059] Properly pass back primitive typed arrays to HTSJDK. [\#2075](https://github.com/bigdatagenomics/adam/pull/2075) ([heuermh](https://github.com/heuermh))
 - Update dependency versions, including htsjdk to 2.16.1 and guava to 27.0-jre [\#2072](https://github.com/bigdatagenomics/adam/pull/2072) ([heuermh](https://github.com/heuermh))
 - [ADAM-1999] Support Python 3 [\#2070](https://github.com/bigdatagenomics/adam/pull/2070) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-2068] Prevent NumberFormatException for nan vs NaN in VCF files. [\#2069](https://github.com/bigdatagenomics/adam/pull/2069) ([heuermh](https://github.com/heuermh))
 - Update python MAKE file [\#2067](https://github.com/bigdatagenomics/adam/pull/2067) ([Georgehe4](https://github.com/Georgehe4))
 - Update python MAKE file [\#2066](https://github.com/bigdatagenomics/adam/pull/2066) ([Georgehe4](https://github.com/Georgehe4))
 - Update jenkins script to test python 3.6 [\#2060](https://github.com/bigdatagenomics/adam/pull/2060) ([Georgehe4](https://github.com/Georgehe4))
 - [ADAM-2062] Update Spark version to 2.3.2 [\#2055](https://github.com/bigdatagenomics/adam/pull/2055) ([heuermh](https://github.com/heuermh))
 - Clean up fields and doc in fragment. [\#2054](https://github.com/bigdatagenomics/adam/pull/2054) ([heuermh](https://github.com/heuermh))
 - [ADAM-2037] Support GFF3 files containing FASTA formatted sequences. [\#2053](https://github.com/bigdatagenomics/adam/pull/2053) ([heuermh](https://github.com/heuermh))
 - modified CoverageRDD and FeatureRDD to extend MultisampleGenomicDataset [\#2051](https://github.com/bigdatagenomics/adam/pull/2051) ([akmorrow13](https://github.com/akmorrow13))
 - Multi-sample coverage [\#2050](https://github.com/bigdatagenomics/adam/pull/2050) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-2047] Use source directory relative to project.basedir for adam codegen. [\#2048](https://github.com/bigdatagenomics/adam/pull/2048) ([heuermh](https://github.com/heuermh))
 - [ADAM-2039] Adding support for writing BED format per UCSC definition [\#2042](https://github.com/bigdatagenomics/adam/pull/2042) ([heuermh](https://github.com/heuermh))
 - Update Jenkins Spark version to 2.2.2 [\#2035](https://github.com/bigdatagenomics/adam/pull/2035) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-2032] Add StorageLevel as an optional parameter to loadPairedFastq [\#2033](https://github.com/bigdatagenomics/adam/pull/2033) ([heuermh](https://github.com/heuermh))
 - [ADAM-2027] Add RDD and Dataset constructors to CoverageRDD. [\#2028](https://github.com/bigdatagenomics/adam/pull/2028) ([heuermh](https://github.com/heuermh))
 - Allow for export of query name sorted SAM files [\#2026](https://github.com/bigdatagenomics/adam/pull/2026) ([karenfeng](https://github.com/karenfeng))
 - [ADAM-2020] Fix ReadTheDocs Github banner. [\#2021](https://github.com/bigdatagenomics/adam/pull/2021) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1988] Add copyVariantEndToAttribute method to support gVCF END attribute … [\#2017](https://github.com/bigdatagenomics/adam/pull/2017) ([heuermh](https://github.com/heuermh))
 - [ADAM-936] Use github-changes-maven-plugin to update CHANGES.md. [\#2014](https://github.com/bigdatagenomics/adam/pull/2014) ([heuermh](https://github.com/heuermh))
 - [ADAM-1992] Make maximum FASTQ read length configurable. [\#2011](https://github.com/bigdatagenomics/adam/pull/2011) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1697] Expand Illumina metadata regex to cover interleaved index sequences. [\#2010](https://github.com/bigdatagenomics/adam/pull/2010) ([heuermh](https://github.com/heuermh))
 - [ADAM-2007] Make IndelRealignmentTarget implement Serializable. [\#2009](https://github.com/bigdatagenomics/adam/pull/2009) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-2006] Support loading 0-length reads as FASTQ. [\#2008](https://github.com/bigdatagenomics/adam/pull/2008) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1697] Expand Illumina metadata regex to cover index sequences [\#2004](https://github.com/bigdatagenomics/adam/pull/2004) ([pauldwolfe](https://github.com/pauldwolfe))
 - [ADAM-1996] Load and save VariantContexts as partitioned Parquet. [\#2001](https://github.com/bigdatagenomics/adam/pull/2001) ([heuermh](https://github.com/heuermh))
 - [ADAM-1997] Nest list of region join types in joins doc. [\#1998](https://github.com/bigdatagenomics/adam/pull/1998) ([heuermh](https://github.com/heuermh))
 - [ADAM-1877] Add filterToReferenceName(s) to SequenceDictionary. [\#1995](https://github.com/bigdatagenomics/adam/pull/1995) ([heuermh](https://github.com/heuermh))
 - [ADAM-1984] Support file systems that don't set the scheme. [\#1985](https://github.com/bigdatagenomics/adam/pull/1985) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1978] Add additional filter by convenience methods. [\#1983](https://github.com/bigdatagenomics/adam/pull/1983) ([heuermh](https://github.com/heuermh))
 - Adding printAttribute methods for alignment records, features, and samples. [\#1982](https://github.com/bigdatagenomics/adam/pull/1982) ([heuermh](https://github.com/heuermh))
 -  Fix partitioning code to use Long instead of Int [\#1980](https://github.com/bigdatagenomics/adam/pull/1980) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1976] Adding core API documentation link and badge. [\#1979](https://github.com/bigdatagenomics/adam/pull/1979) ([heuermh](https://github.com/heuermh))
 - [ADAM-1974] Close unclosed stream in FastqInputFormat. [\#1975](https://github.com/bigdatagenomics/adam/pull/1975) ([fnothaft](https://github.com/fnothaft))
 - Set defaults to schemas [\#1972](https://github.com/bigdatagenomics/adam/pull/1972) ([ffinfo](https://github.com/ffinfo))
 - Add loadPairedFastqAsFragments method. [\#1866](https://github.com/bigdatagenomics/adam/pull/1866) ([heuermh](https://github.com/heuermh))
 - Adding loadPairedFastqAsFragments method [\#1828](https://github.com/bigdatagenomics/adam/pull/1828) ([ffinfo](https://github.com/ffinfo))


### Version 0.24.0 ###

**Closed issues:**

 - Phred values from 156–254 do not round trip properly between log space [\#1964](https://github.com/bigdatagenomics/adam/issues/1964)
 - Support VCF lines with positions at 0 [\#1959](https://github.com/bigdatagenomics/adam/issues/1959)
 - Don't initialize non-ref values to Int.MinValue [\#1957](https://github.com/bigdatagenomics/adam/issues/1957)
 - Support downsampling in recalibration [\#1955](https://github.com/bigdatagenomics/adam/issues/1955)
 - Cannot waive validation stringency for INFO Number=.,Type=Flag fields [\#1939](https://github.com/bigdatagenomics/adam/issues/1939)
 - Clip phred scores below Int.MaxValue [\#1934](https://github.com/bigdatagenomics/adam/issues/1934)
 - ADAMContext.getFsAndFilesWithFilter should throw exception if paths null or empty [\#1932](https://github.com/bigdatagenomics/adam/issues/1932)
 - Bump to Spark 2.3.0 [\#1931](https://github.com/bigdatagenomics/adam/issues/1931)
 - util.FileExtensions should be public for use downstream in Cannoli [\#1927](https://github.com/bigdatagenomics/adam/issues/1927)
 - Reduce logging level for ADAMKryoRegistrator [\#1925](https://github.com/bigdatagenomics/adam/issues/1925)
 - Revisit performance implications of commit 1eed8e8 [\#1923](https://github.com/bigdatagenomics/adam/issues/1923)
 - add akmorrow13 to PyPl for bdgenomics.adam [\#1919](https://github.com/bigdatagenomics/adam/issues/1919)
 - Read the Docs build failing with TypeError: super() argument 1 must be type, not None [\#1917](https://github.com/bigdatagenomics/adam/issues/1917)
 - Bump Hadoop-BAM dependency to 7.9.2. [\#1915](https://github.com/bigdatagenomics/adam/issues/1915)
 - cannot run pyadam from adam distribution 0.23.0 [\#1914](https://github.com/bigdatagenomics/adam/issues/1914)
 - adam2fasta/q are missing asSingleFile, disableFastConcat [\#1912](https://github.com/bigdatagenomics/adam/issues/1912)
 - Pipe API doesn't properly handle multiple arguments and spaces [\#1909](https://github.com/bigdatagenomics/adam/issues/1909)
 - Bump to HTSJDK 2.13.2 [\#1907](https://github.com/bigdatagenomics/adam/issues/1907)
 - S3A error: HTTP request: Timeout waiting for connection from pool [\#1906](https://github.com/bigdatagenomics/adam/issues/1906)
 - InputStream passed to VCFHeaderReader does not get closed [\#1900](https://github.com/bigdatagenomics/adam/issues/1900)
 - Support INFO fields set to missing [\#1898](https://github.com/bigdatagenomics/adam/issues/1898)
 - CLI to transfer between cloud storage and HDFS [\#1896](https://github.com/bigdatagenomics/adam/issues/1896)
 - Jenkins does not run python or R tests [\#1889](https://github.com/bigdatagenomics/adam/issues/1889)
 - pyadam throws application option error [\#1886](https://github.com/bigdatagenomics/adam/issues/1886)
 - ReferenceRegion in python does not exist [\#1884](https://github.com/bigdatagenomics/adam/issues/1884)
 - Caching GenomicRDD in pyspark [\#1883](https://github.com/bigdatagenomics/adam/issues/1883)
 - adam-submit aborts if ADAM_HOME is set [\#1882](https://github.com/bigdatagenomics/adam/issues/1882)
 - Allow piped commands to timeout [\#1875](https://github.com/bigdatagenomics/adam/issues/1875)
 - loadVcf does not dedupe sample ID [\#1874](https://github.com/bigdatagenomics/adam/issues/1874)
 - Add coverage command for reporting read coverage [\#1873](https://github.com/bigdatagenomics/adam/issues/1873)
 - Only python 2?  [\#1871](https://github.com/bigdatagenomics/adam/issues/1871)
 - Support VariantContextRDD from SQL [\#1867](https://github.com/bigdatagenomics/adam/issues/1867)
 - Cannot find `find-adam-assembly.sh` in bioconda build [\#1862](https://github.com/bigdatagenomics/adam/issues/1862)
 - `_jvm.java.lang.Class.forName` does not work for certain configurations [\#1858](https://github.com/bigdatagenomics/adam/issues/1858)
 - Formatting error in CHANGES.md [\#1857](https://github.com/bigdatagenomics/adam/issues/1857)
 - Various improvements to readthedocs documentation [\#1853](https://github.com/bigdatagenomics/adam/issues/1853)
 - add filterByOverlappingRegion(query: ReferenceRegion) to R and python APIs [\#1852](https://github.com/bigdatagenomics/adam/issues/1852)
 - Support adding VCF header lines from Python [\#1840](https://github.com/bigdatagenomics/adam/issues/1840)
 - Support loadIndexedBam from Python [\#1836](https://github.com/bigdatagenomics/adam/issues/1836)
 - Add link to awesome list of applications that extend ADAM [\#1832](https://github.com/bigdatagenomics/adam/issues/1832)
 - loadIndexed bam lazily throws Exception if index does not exist [\#1830](https://github.com/bigdatagenomics/adam/issues/1830)
 - OAuth credentials for Github in Coveralls configuration are no longer valid [\#1829](https://github.com/bigdatagenomics/adam/issues/1829)
 - base counts per position [\#1825](https://github.com/bigdatagenomics/adam/issues/1825)
 - Issues loading BAM files in Google FS [\#1816](https://github.com/bigdatagenomics/adam/issues/1816)
 - Error when writing a vcf file to Parquet [\#1810](https://github.com/bigdatagenomics/adam/issues/1810)
 - transformAlignments cannot repartition files [\#1808](https://github.com/bigdatagenomics/adam/issues/1808)
 - GenotypeRDD should support `toVariants` method [\#1806](https://github.com/bigdatagenomics/adam/issues/1806)
 - Add support for python and R in Homebrew formula [\#1796](https://github.com/bigdatagenomics/adam/issues/1796)
 - Add `transformVariantContexts` or similar to cli [\#1793](https://github.com/bigdatagenomics/adam/issues/1793)
 - Issue while using Sorting option [\#1791](https://github.com/bigdatagenomics/adam/issues/1791)
 - Issue with adam2vcf [\#1787](https://github.com/bigdatagenomics/adam/issues/1787)
 - Remove explicit `<compile>` scopes from submodule POMs [\#1786](https://github.com/bigdatagenomics/adam/issues/1786)
 - java.nio.file.ProviderNotFoundException (Provider "s3" not found) [\#1732](https://github.com/bigdatagenomics/adam/issues/1732)
 - Accessing GenomicRDD join functions in python [\#1728](https://github.com/bigdatagenomics/adam/issues/1728)
 - ArrayIndexOutOfBoundsException in PhredUtils$.phredToSuccessProbability [\#1714](https://github.com/bigdatagenomics/adam/issues/1714)
 - Add ability to specify region bounds to pipe command [\#1707](https://github.com/bigdatagenomics/adam/issues/1707)
 - Unable to run pyadam, SQLException: Failed to start database 'metastore_db' [\#1666](https://github.com/bigdatagenomics/adam/issues/1666)
 - SAMFormatException: Unrecognized tag type: ^@ [\#1657](https://github.com/bigdatagenomics/adam/issues/1657)
 - IndexOutOfBoundsException in BAMInputFormat.getSplits [\#1656](https://github.com/bigdatagenomics/adam/issues/1656)
 - overlaps considers that Strand.FORWARD cannot overlap with Strand.INDEPENDENT [\#1650](https://github.com/bigdatagenomics/adam/issues/1650)
 - migration converters [\#1629](https://github.com/bigdatagenomics/adam/issues/1629)
 - RFC: Removing Spark 1.x, Scala 2.10 support in 0.24.0 release [\#1597](https://github.com/bigdatagenomics/adam/issues/1597)
 - Eliminate unused ConcreteADAMRDDFunctions class [\#1580](https://github.com/bigdatagenomics/adam/issues/1580)
 - Add set theory/statistics packages to ADAM [\#1533](https://github.com/bigdatagenomics/adam/issues/1533)
 - Evaluate Apache Carbondata INDEXED column store file format for genomics [\#1527](https://github.com/bigdatagenomics/adam/issues/1527)
 - Stranded vs unstranded in getReferenceRegions() for features [\#1513](https://github.com/bigdatagenomics/adam/issues/1513)
 - Question:How to tranform a line of sam to AlignmentRecord? [\#1425](https://github.com/bigdatagenomics/adam/issues/1425)
 - Excessive compilation warnings about multiple scala libraries [\#695](https://github.com/bigdatagenomics/adam/issues/695)
 - Support Hive-style partitioning [\#651](https://github.com/bigdatagenomics/adam/issues/651)

**Merged and closed pull requests:**

 - [ADAM-1964] Lower point where phred conversions are done using log code. [\#1965](https://github.com/bigdatagenomics/adam/pull/1965) ([fnothaft](https://github.com/fnothaft))
 - Add utility methods for adam-shell. [\#1958](https://github.com/bigdatagenomics/adam/pull/1958) ([heuermh](https://github.com/heuermh))
 - [ADAM-1955] Add support for downsampling during recalibration table generation [\#1963](https://github.com/bigdatagenomics/adam/pull/1963) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1957] Don't initialize missing likelihoods to MinValue. [\#1961](https://github.com/bigdatagenomics/adam/pull/1961) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1959] Support VCF rows at position 0. [\#1960](https://github.com/bigdatagenomics/adam/pull/1960) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-651] Implement Hive-style partitioning by genomic range of Parquet backed datasets [\#1948](https://github.com/bigdatagenomics/adam/pull/1948) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1914] Python profile needs to be specified for egg to be in distribution. [\#1946](https://github.com/bigdatagenomics/adam/pull/1946) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1917] Delete dependency on fulltoc. [\#1944](https://github.com/bigdatagenomics/adam/pull/1944) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1917] Try 3: fix Sphinx fulltoc. [\#1943](https://github.com/bigdatagenomics/adam/pull/1943) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1917] Set Sphinx version in requirements.txt. [\#1942](https://github.com/bigdatagenomics/adam/pull/1942) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1917] Set minimal Sphinx version for Readthedocs build. [\#1941](https://github.com/bigdatagenomics/adam/pull/1941) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1939] Allow validation stringency to waive off FLAG arrays. [\#1940](https://github.com/bigdatagenomics/adam/pull/1940) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1915] Bump to Hadoop-BAM 7.9.2. [\#1938](https://github.com/bigdatagenomics/adam/pull/1938) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1934] Clip phred values to 3233, instead of Int.MaxValue. [\#1936](https://github.com/bigdatagenomics/adam/pull/1936) ([fnothaft](https://github.com/fnothaft))
 - Ignore VCF INFO fields with number=G when stringency=LENIENT [\#1935](https://github.com/bigdatagenomics/adam/pull/1935) ([jpdna](https://github.com/jpdna))
 - [ADAM-1931] Bump to Spark 2.3.0. [\#1933](https://github.com/bigdatagenomics/adam/pull/1933) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1840] Support adding VCF header lines from Python. [\#1930](https://github.com/bigdatagenomics/adam/pull/1930) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1927] Increase visibility for util.FileExtensions for use downstream. [\#1929](https://github.com/bigdatagenomics/adam/pull/1929) ([heuermh](https://github.com/heuermh))
 - [ADAM-1925] Reduce logging level for ADAMKryoRegistrator. [\#1928](https://github.com/bigdatagenomics/adam/pull/1928) ([heuermh](https://github.com/heuermh))
 - [ADAM-1923] Revert 1eed8e8 [\#1926](https://github.com/bigdatagenomics/adam/pull/1926) ([fnothaft](https://github.com/fnothaft))
 - Use SparkFiles.getRootDirectory in local mode. [\#1924](https://github.com/bigdatagenomics/adam/pull/1924) ([heuermh](https://github.com/heuermh))
 - [ADAM-651] Implement Hive-style partitioning by genomic range of Parquet backed datasets [\#1922](https://github.com/bigdatagenomics/adam/pull/1922) ([jpdna](https://github.com/jpdna))
 - Make Spark SQL APIs supported across all types [\#1921](https://github.com/bigdatagenomics/adam/pull/1921) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1909] Refactor pipe cmd parameter from String to Seq[String]. [\#1920](https://github.com/bigdatagenomics/adam/pull/1920) ([heuermh](https://github.com/heuermh))
 - Add Google Cloud documentation [\#1918](https://github.com/bigdatagenomics/adam/pull/1918) ([Georgehe4](https://github.com/Georgehe4))
 - [ADAM-1917] Load sphinxcontrib.fulltoc with imp.load_sources. [\#1916](https://github.com/bigdatagenomics/adam/pull/1916) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1912] Add asSingleFile, disableFastConcat to adam2fasta/q. [\#1913](https://github.com/bigdatagenomics/adam/pull/1913) ([heuermh](https://github.com/heuermh))
 - [ADAM-651] Hive-style partitioning of parquet files by genomic position [\#1911](https://github.com/bigdatagenomics/adam/pull/1911) ([jpdna](https://github.com/jpdna))
 - Minor unit test/style fixes. [\#1910](https://github.com/bigdatagenomics/adam/pull/1910) ([heuermh](https://github.com/heuermh))
 - [ADAM-1907] Bump to HTSJDK 2.13.2. [\#1908](https://github.com/bigdatagenomics/adam/pull/1908) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1882] Don't abort adam-submit if ADAM_HOME is set. [\#1905](https://github.com/bigdatagenomics/adam/pull/1905) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1806] Add toVariants conversion from GenotypeRDD. [\#1904](https://github.com/bigdatagenomics/adam/pull/1904) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1882] Return true if ADAM_HOME is set, not exit 0. [\#1903](https://github.com/bigdatagenomics/adam/pull/1903) ([heuermh](https://github.com/heuermh))
 - [ADAM-1900] Close stream after reading VCF header. [\#1901](https://github.com/bigdatagenomics/adam/pull/1901) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1898] Support converting INFO fields set to empty ('.'). [\#1899](https://github.com/bigdatagenomics/adam/pull/1899) ([fnothaft](https://github.com/fnothaft))
 - Add Kryo registration for two classes required for Spark 2.3.0. [\#1897](https://github.com/bigdatagenomics/adam/pull/1897) ([jpdna](https://github.com/jpdna))
 - [ADAM-1853] Various improvements to readthedocs documentation. [\#1893](https://github.com/bigdatagenomics/adam/pull/1893) ([heuermh](https://github.com/heuermh))
 - [ADAM-1889][ADAM-1884] updated ReferenceRegion in python [\#1892](https://github.com/bigdatagenomics/adam/pull/1892) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1889] Run R/Python tests. [\#1890](https://github.com/bigdatagenomics/adam/pull/1890) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1886] fix for pyadam to recognize >1 egg file [\#1887](https://github.com/bigdatagenomics/adam/pull/1887) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1883] Python and R caching [\#1885](https://github.com/bigdatagenomics/adam/pull/1885) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1875] Add ability to timeout a piped command. [\#1881](https://github.com/bigdatagenomics/adam/pull/1881) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1871] Fix print call that broke python 3 support. [\#1880](https://github.com/bigdatagenomics/adam/pull/1880) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1832] Use awesome list style and link to bigdatagenomics/awesome-adam. [\#1879](https://github.com/bigdatagenomics/adam/pull/1879) ([heuermh](https://github.com/heuermh))
 - [ADAM-651] Hive-style partitioning of parquet files by genomic position [\#1878](https://github.com/bigdatagenomics/adam/pull/1878) ([jpdna](https://github.com/jpdna))
 - [ADAM-1874] Dedupe samples when loading VCFs. [\#1876](https://github.com/bigdatagenomics/adam/pull/1876) ([fnothaft](https://github.com/fnothaft))
 - Fixes Coverage python API and adds tests [\#1870](https://github.com/bigdatagenomics/adam/pull/1870) ([akmorrow13](https://github.com/akmorrow13))
 - added filterByOverlappingRegion for python [\#1869](https://github.com/bigdatagenomics/adam/pull/1869) ([akmorrow13](https://github.com/akmorrow13))
 - Add command line option for populating nested variant.annotation field in Genotype records. [\#1865](https://github.com/bigdatagenomics/adam/pull/1865) ([heuermh](https://github.com/heuermh))
 - Hive partitioned(v4) rebased [\#1864](https://github.com/bigdatagenomics/adam/pull/1864) ([jpdna](https://github.com/jpdna))
 - [ADAM-1597] Move to Scala 2.11 and Spark 2.x. [\#1861](https://github.com/bigdatagenomics/adam/pull/1861) ([heuermh](https://github.com/heuermh))
 - [ADAM-1857] Fix formatting error due to forward slashes. [\#1860](https://github.com/bigdatagenomics/adam/pull/1860) ([heuermh](https://github.com/heuermh))
 - [ADAM-1858] Use getattr instead of Class.forName from python API. [\#1859](https://github.com/bigdatagenomics/adam/pull/1859) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1836] Adds loadIndexedBam API to Python and Java. [\#1837](https://github.com/bigdatagenomics/adam/pull/1837) ([fnothaft](https://github.com/fnothaft))
 - Added check for bam index files in loadIndexedBam [\#1831](https://github.com/bigdatagenomics/adam/pull/1831) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1793] Adding vcf2adam and adam2vcf that handle separate variant and genotype data. [\#1794](https://github.com/bigdatagenomics/adam/pull/1794) ([heuermh](https://github.com/heuermh))
 - added adam notebook [\#1778](https://github.com/bigdatagenomics/adam/pull/1778) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1666] SQLContext creation fix for Spark 2.x [\#1777](https://github.com/bigdatagenomics/adam/pull/1777) ([akmorrow13](https://github.com/akmorrow13))
 - Add optional accumulator for VCF header lines to VCFOutFormatter. [\#1727](https://github.com/bigdatagenomics/adam/pull/1727) ([heuermh](https://github.com/heuermh))
 - add hive style partitioning for contigName [\#1620](https://github.com/bigdatagenomics/adam/pull/1620) ([jpdna](https://github.com/jpdna))
 - Add loadReadsFromSamString function into ADAMContext [\#1434](https://github.com/bigdatagenomics/adam/pull/1434) ([xubo245](https://github.com/xubo245))


### Version 0.23.0 ###

**Closed issues:**

 - Readthedocs build error [\#1854](https://github.com/bigdatagenomics/adam/issues/1854)
 - Add pip release to release scripts [\#1847](https://github.com/bigdatagenomics/adam/issues/1847)
 - Publish scaladoc script still attempts to build markdown docs [\#1845](https://github.com/bigdatagenomics/adam/issues/1845)
 - Allow variant annotations to be loaded into genotypes [\#1838](https://github.com/bigdatagenomics/adam/issues/1838)
 - Specify correct extensions for SAM/BAM output [\#1834](https://github.com/bigdatagenomics/adam/issues/1834)
 - Fix link anchors and other issues in readthedocs [\#1822](https://github.com/bigdatagenomics/adam/issues/1822)
 - Sphinx fulltoc is not included [\#1821](https://github.com/bigdatagenomics/adam/issues/1821)
 - Readme link to bigdatagenomics/lime 404s [\#1819](https://github.com/bigdatagenomics/adam/issues/1819)
 - Bump to Hadoop-BAM 7.9.1 [\#1817](https://github.com/bigdatagenomics/adam/issues/1817)
 - LoadVariants Header Format [\#1815](https://github.com/bigdatagenomics/adam/issues/1815)
 - Right and Left Outer Shuffle Region Join don't match [\#1813](https://github.com/bigdatagenomics/adam/issues/1813)
 - Pipe command can fail with empty partitions [\#1807](https://github.com/bigdatagenomics/adam/issues/1807)
 - adam files with outdated formats throw FileNotFoundException [\#1804](https://github.com/bigdatagenomics/adam/issues/1804)
 - Move GenomicRDD.writeTextRDD outside of GenomicRDD [\#1803](https://github.com/bigdatagenomics/adam/issues/1803)
 - find-adam-assembly fails to recognize more than 1 jar [\#1801](https://github.com/bigdatagenomics/adam/issues/1801)
 - tests/testthat.R failed on git head [\#1799](https://github.com/bigdatagenomics/adam/issues/1799)
 - Run python and R tests conditionally in build [\#1795](https://github.com/bigdatagenomics/adam/issues/1795)
 - scala-lang should be a provided dependency [\#1789](https://github.com/bigdatagenomics/adam/issues/1789)
 - loadIndexedBam does an unnecessary union [\#1784](https://github.com/bigdatagenomics/adam/issues/1784)
 - Release bdgenomics.adam R package on CRAN [\#1783](https://github.com/bigdatagenomics/adam/issues/1783)
 - Issue with transformVariant // Adam to vcf [\#1782](https://github.com/bigdatagenomics/adam/issues/1782)
 - Add code of conduct [\#1779](https://github.com/bigdatagenomics/adam/issues/1779)
 - Reinstantiation of SQLContext in pyadam ADAMContext [\#1774](https://github.com/bigdatagenomics/adam/issues/1774)
 - Genotypes should only contain the core variant fields [\#1770](https://github.com/bigdatagenomics/adam/issues/1770)
 - Add SingleFASTQInFormatter [\#1768](https://github.com/bigdatagenomics/adam/issues/1768)
 - INDEL realigner can emit negative partition IDs [\#1763](https://github.com/bigdatagenomics/adam/issues/1763)
 - Request for a new release [\#1762](https://github.com/bigdatagenomics/adam/issues/1762)
 - INDEL realigner generates targets for reads with more than 1 INDEL [\#1753](https://github.com/bigdatagenomics/adam/issues/1753)
 - Fragment Issue [\#1752](https://github.com/bigdatagenomics/adam/issues/1752)
 - Variant Caller!!! [\#1751](https://github.com/bigdatagenomics/adam/issues/1751)
 - Spark Version!! [\#1750](https://github.com/bigdatagenomics/adam/issues/1750)
 - ReferenceRegion.subtract eliminating valid regions [\#1747](https://github.com/bigdatagenomics/adam/issues/1747)
 - New Shuffle Join Implementation - Left Outer + Group By Left [\#1745](https://github.com/bigdatagenomics/adam/issues/1745)
 - command failure after build success [\#1744](https://github.com/bigdatagenomics/adam/issues/1744)
 - Recalibrate_base_Qualities [\#1743](https://github.com/bigdatagenomics/adam/issues/1743)
 - Standardize regionFn for ShuffleJoin returned objects [\#1740](https://github.com/bigdatagenomics/adam/issues/1740)
 - Shuffle, Broadcast Joins with threshold [\#1739](https://github.com/bigdatagenomics/adam/issues/1739)
 - Adam on Spark 2.1 [\#1738](https://github.com/bigdatagenomics/adam/issues/1738)
 - Opening up permission on GenericGenomicRDD constructor [\#1735](https://github.com/bigdatagenomics/adam/issues/1735)
 - Consistency on ShuffleRegionJoin returns [\#1734](https://github.com/bigdatagenomics/adam/issues/1734)
 - vcf2adam support [\#1731](https://github.com/bigdatagenomics/adam/issues/1731)
 - Cloud-scale BWA MEM [\#1730](https://github.com/bigdatagenomics/adam/issues/1730)
 - Aligned Human Genome couldn't convert to Adam  [\#1729](https://github.com/bigdatagenomics/adam/issues/1729)
 - Mark Duplicates [\#1726](https://github.com/bigdatagenomics/adam/issues/1726)
 - Genomics Pipeline [\#1724](https://github.com/bigdatagenomics/adam/issues/1724)
 - .fastq Alignment  [\#1723](https://github.com/bigdatagenomics/adam/issues/1723)
 - Is it correct Adam file [\#1720](https://github.com/bigdatagenomics/adam/issues/1720)
 - .fastQ to .adam [\#1718](https://github.com/bigdatagenomics/adam/issues/1718)
 - Unable to create .adam from .sam [\#1717](https://github.com/bigdatagenomics/adam/issues/1717)
 - Add adam- prefix to distribution module name [\#1716](https://github.com/bigdatagenomics/adam/issues/1716)
 - Python load methods don't have ability to specify validation stringency [\#1715](https://github.com/bigdatagenomics/adam/issues/1715)
 - NPE when trying to map *loadVariants* over RDD [\#1713](https://github.com/bigdatagenomics/adam/issues/1713)
 - Add left normalization of INDELs as an RDD level primitive [\#1709](https://github.com/bigdatagenomics/adam/issues/1709)
 - Allow validation stringency to be set in AnySAMOutFormatter [\#1703](https://github.com/bigdatagenomics/adam/issues/1703)
 - InterleavedFastqInFormatter should sort by readInFragment [\#1702](https://github.com/bigdatagenomics/adam/issues/1702)
 - Allow silencing the # of reads in fragment warning in InterleavedFastqInFormatter [\#1701](https://github.com/bigdatagenomics/adam/issues/1701)
 - GenomicRDD.toXxx method names should be consistent [\#1699](https://github.com/bigdatagenomics/adam/issues/1699)
 - Exception thrown in VariantContextConverter.formatAllelicDepth despite SILENT validation stringency [\#1695](https://github.com/bigdatagenomics/adam/issues/1695)
 - Make GenomicRDD.toString more adam-shell friendly [\#1694](https://github.com/bigdatagenomics/adam/issues/1694)
 - Add adam-shell friendly VariantContextRDD.saveAsVcf method [\#1693](https://github.com/bigdatagenomics/adam/issues/1693)
 - change bdgenomics.adam package name for adam-python to bdg-adam [\#1691](https://github.com/bigdatagenomics/adam/issues/1691)
 - Conflict in bdg-formats dependency version due to org.hammerlab:genomic-loci [\#1688](https://github.com/bigdatagenomics/adam/issues/1688)
 - Convert and store variant quality field. [\#1682](https://github.com/bigdatagenomics/adam/issues/1682)
 - Region join shows non-determinism [\#1680](https://github.com/bigdatagenomics/adam/issues/1680)
 - Shuffle region join throws multimapped exception for unmapped reads [\#1679](https://github.com/bigdatagenomics/adam/issues/1679)
 - Push validation checks down to INFO/FORMAT fields [\#1676](https://github.com/bigdatagenomics/adam/issues/1676)
 - IndexOutOfBounds thrown when saving gVCF with no likelihoods [\#1673](https://github.com/bigdatagenomics/adam/issues/1673)
 - Generate docs from R API for distribution [\#1672](https://github.com/bigdatagenomics/adam/issues/1672)
 - Support loading a subset of VCF fields [\#1670](https://github.com/bigdatagenomics/adam/issues/1670)
 - Error with metadata: Multivalued flags are not supported for INFO lines [\#1669](https://github.com/bigdatagenomics/adam/issues/1669)
 - Include bdg.adam-0.23.0.tar.gz in distribution tarballs [\#1668](https://github.com/bigdatagenomics/adam/issues/1668)
 - Include bdgenomics.adam-0.23.0_SNAPSHOT-py2.7.egg in distribution tarball [\#1667](https://github.com/bigdatagenomics/adam/issues/1667)
 - Add SUPPORT.md file to complement CONTRIBUTING.md [\#1664](https://github.com/bigdatagenomics/adam/issues/1664)
 - Can't merge BAM files containing the same sample [\#1663](https://github.com/bigdatagenomics/adam/issues/1663)
 - Incorrect README.md  kmer.scala loadAliments method parameter name [\#1662](https://github.com/bigdatagenomics/adam/issues/1662)
 - Add performance benchmarks similar to Samtools CRAM benchmarking page [\#1661](https://github.com/bigdatagenomics/adam/issues/1661)
 - Transient bad GZIP header bug when loading BGZF FASTQ [\#1658](https://github.com/bigdatagenomics/adam/issues/1658)
 - bdgenomics.adam vs bdg.adam for R/Python APIs [\#1655](https://github.com/bigdatagenomics/adam/issues/1655)
 - Need adamR script [\#1649](https://github.com/bigdatagenomics/adam/issues/1649)
 - incorrect grep for assembly jars in bin/pyadam [\#1647](https://github.com/bigdatagenomics/adam/issues/1647)
 - VariantRDD union creates multiple records for the same SNP ID [\#1644](https://github.com/bigdatagenomics/adam/issues/1644)
 - S3 access documentation [\#1643](https://github.com/bigdatagenomics/adam/issues/1643)
 - Algorithms docs formatting [\#1639](https://github.com/bigdatagenomics/adam/issues/1639)
 - Building downstream apps docs reformatting [\#1638](https://github.com/bigdatagenomics/adam/issues/1638)
 - FastqInputFormat.FILE_SPLITTABLE in conf not getting passed properly [\#1635](https://github.com/bigdatagenomics/adam/issues/1635)
 - Add benchmarks to documentation [\#1634](https://github.com/bigdatagenomics/adam/issues/1634)
 - Intro docs contain outdated/incompatible code [\#1633](https://github.com/bigdatagenomics/adam/issues/1633)
 - Intro docs missing a number of active projects [\#1632](https://github.com/bigdatagenomics/adam/issues/1632)
 - Installation instructions for Homebrew missing from documentation [\#1631](https://github.com/bigdatagenomics/adam/issues/1631)
 - Architecture section is missing from docs [\#1630](https://github.com/bigdatagenomics/adam/issues/1630)
 - Seq<VCFCompoundHeaderLine> vs. Seq<VCFHeaderLine> with javac [\#1625](https://github.com/bigdatagenomics/adam/issues/1625)
 - ProcessingStep missing from adam-codegen [\#1623](https://github.com/bigdatagenomics/adam/issues/1623)
 - Add ADAM recipe to bioconda [\#1618](https://github.com/bigdatagenomics/adam/issues/1618)
 - adam-submit cannot find assembly jar if installed as symlink [\#1616](https://github.com/bigdatagenomics/adam/issues/1616)
 - Expose transform/transmute in Java/Python/R [\#1615](https://github.com/bigdatagenomics/adam/issues/1615)
 - Expose VariantContextRDD in R/Python [\#1614](https://github.com/bigdatagenomics/adam/issues/1614)
 - Expose pipe API from Python/R [\#1611](https://github.com/bigdatagenomics/adam/issues/1611)
 - Serialization issue with TwoBitFile [\#1610](https://github.com/bigdatagenomics/adam/issues/1610)
 - Snapshot Distribution Does not include jar files [\#1607](https://github.com/bigdatagenomics/adam/issues/1607)
 - ManualRegionPartitioner is broken for ParallelFileMerger codepath [\#1602](https://github.com/bigdatagenomics/adam/issues/1602)
 - VariantRDD doesn't save partition map [\#1601](https://github.com/bigdatagenomics/adam/issues/1601)
 - Scala copy method not supported in abstract classes such as AlignmentRecordRDD [\#1599](https://github.com/bigdatagenomics/adam/issues/1599)
 - Interleaved FASTQ recognizes only /1 suffix pattern [\#1589](https://github.com/bigdatagenomics/adam/issues/1589)
 - Use empty sequence dictionary when loading features [\#1588](https://github.com/bigdatagenomics/adam/issues/1588)
 - New Illumina FASTQ spec adds metadata to read name line [\#1585](https://github.com/bigdatagenomics/adam/issues/1585)
 - first run of ADAM [\#1582](https://github.com/bigdatagenomics/adam/issues/1582)
 - Add unit test coverage for BED12 parser and writer [\#1579](https://github.com/bigdatagenomics/adam/issues/1579)
 - Spark 1.x Scala 2.10 snapshot artifacts missing since 31 March 2017 [\#1578](https://github.com/bigdatagenomics/adam/issues/1578)
 - Unable to save GenomicRDDs after a join. [\#1576](https://github.com/bigdatagenomics/adam/issues/1576)
 - Add filterBySequenceDictionary to GenomicRDD [\#1575](https://github.com/bigdatagenomics/adam/issues/1575)
 - Unaligned Trait does nothing [\#1573](https://github.com/bigdatagenomics/adam/issues/1573)
 - Bump to bdg-formats 0.11.1 [\#1570](https://github.com/bigdatagenomics/adam/issues/1570)
 - PhredUtils conversion to log probabilities has insufficient resolution for PLs [\#1569](https://github.com/bigdatagenomics/adam/issues/1569)
 - Reference model import code is borked [\#1568](https://github.com/bigdatagenomics/adam/issues/1568)
 - SequenceDictionary vs Feature[RDD] of reference length features [\#1567](https://github.com/bigdatagenomics/adam/issues/1567)
 - giab-NA12878 truth_small_variants.vcf.gz header issues [\#1566](https://github.com/bigdatagenomics/adam/issues/1566)
 - VCF header read from stream ignored in VCFOutFormatter [\#1564](https://github.com/bigdatagenomics/adam/issues/1564)
 - VCF genotype Number=A attribute throws ArrayIndexOutOfBoundsException [\#1562](https://github.com/bigdatagenomics/adam/issues/1562)
 - Save compressed single file VCF via HadoopBAM [\#1554](https://github.com/bigdatagenomics/adam/issues/1554)
 - bucketing strategy [\#1553](https://github.com/bigdatagenomics/adam/issues/1553)
 - Is parquet using delta encoding for positions? [\#1552](https://github.com/bigdatagenomics/adam/issues/1552)
 - Export to VCF does not include symbolic non-ref if site has a called alt [\#1551](https://github.com/bigdatagenomics/adam/issues/1551)
 - Refactor filterByOverlappingRegions not to require a List [\#1549](https://github.com/bigdatagenomics/adam/issues/1549)
 - Move docs to Sphinx/pure Markdown [\#1548](https://github.com/bigdatagenomics/adam/issues/1548)
 - java.lang.IncompatibleClassChangeError: Implementing class [\#1544](https://github.com/bigdatagenomics/adam/issues/1544)
 - Support locus predicate in `TransformAlignments` [\#1539](https://github.com/bigdatagenomics/adam/issues/1539)
 - Visibility from Java, jrdd has private access in AvroGenomicRDD [\#1538](https://github.com/bigdatagenomics/adam/issues/1538)
 - Rename o.b.adam.apis.java package to o.b.adam.api.java [\#1537](https://github.com/bigdatagenomics/adam/issues/1537)
 - VCF header genotype reserved key FT cardinality clobbered by htsjdk [\#1535](https://github.com/bigdatagenomics/adam/issues/1535)
 - Compute a SequenceDictionary from a *.genome file [\#1534](https://github.com/bigdatagenomics/adam/issues/1534)
 - Queryname sorted check should check for queryname grouped as well [\#1530](https://github.com/bigdatagenomics/adam/issues/1530)
 - Bump to bdg-formats 0.11.0 [\#1520](https://github.com/bigdatagenomics/adam/issues/1520)
 - Move to Spark 2.2, Parquet 1.8.2 [\#1517](https://github.com/bigdatagenomics/adam/issues/1517)
 - Minor refactor for TreeRegionJoin for consistency [\#1514](https://github.com/bigdatagenomics/adam/issues/1514)
 - Allow +Inf and -Inf Float values when reading VCF [\#1512](https://github.com/bigdatagenomics/adam/issues/1512)
 - SparkFiles temp directory path should be accessible as a variable [\#1510](https://github.com/bigdatagenomics/adam/issues/1510)
 - SparkFiles.get expects just the filename [\#1509](https://github.com/bigdatagenomics/adam/issues/1509)
 - Split apart #1324 [\#1507](https://github.com/bigdatagenomics/adam/issues/1507)
 - Where can I find "Phred-scaled quality score" (QUAL)? [\#1506](https://github.com/bigdatagenomics/adam/issues/1506)
 - Alignment Record sort is not consistent with samtools [\#1504](https://github.com/bigdatagenomics/adam/issues/1504)
 - Sequence dictionary records in TwoBitFile are not stable [\#1502](https://github.com/bigdatagenomics/adam/issues/1502)
 - Move coverage counter over to Dataset API [\#1501](https://github.com/bigdatagenomics/adam/issues/1501)
 - Allow users to set the minimum partition count across all load methods [\#1500](https://github.com/bigdatagenomics/adam/issues/1500)
 - Enable reuse of broadcast object across broadcast region joins [\#1499](https://github.com/bigdatagenomics/adam/issues/1499)
 - Take union across genomic RDDs [\#1497](https://github.com/bigdatagenomics/adam/issues/1497)
 - Adam files created by vcf2adam is not recognizable [\#1496](https://github.com/bigdatagenomics/adam/issues/1496)
 - Scalatest log output disappears with Maven 3.5.0 [\#1495](https://github.com/bigdatagenomics/adam/issues/1495)
 - ArrayOutOfBoundsException in vcf2adam (spark2_2.11-0.22.0) on UK10K VCFs (VCFv4.1) [\#1494](https://github.com/bigdatagenomics/adam/issues/1494)
 - ReferenceRegion overlaps and covers returns false if overlap is 1 [\#1492](https://github.com/bigdatagenomics/adam/issues/1492)
 - Provide asSingleFile parameter for saveAsFastq and related [\#1490](https://github.com/bigdatagenomics/adam/issues/1490)
 - Min Phred score gets bumped by 33 twice in BQSR [\#1488](https://github.com/bigdatagenomics/adam/issues/1488)
 - Should throw error when BAM header load fails [\#1486](https://github.com/bigdatagenomics/adam/issues/1486)
 - Default value for reads.toCoverage(collapse) should be false [\#1483](https://github.com/bigdatagenomics/adam/issues/1483)
 - Refactor ADAMContext loadXxx methods for consistency [\#1481](https://github.com/bigdatagenomics/adam/issues/1481)
 - loadGenotypes three time [\#1480](https://github.com/bigdatagenomics/adam/issues/1480)
 - Fall back to sequential concat when HDFS concat fails [\#1478](https://github.com/bigdatagenomics/adam/issues/1478)
 - VCF line with `.` ALT gets dropped [\#1476](https://github.com/bigdatagenomics/adam/issues/1476)
 - ADAM works on Cloudera but does NOT work on MAPR [\#1475](https://github.com/bigdatagenomics/adam/issues/1475)
 - Clean up ReferenceRegion.scala [\#1474](https://github.com/bigdatagenomics/adam/issues/1474)
 - Allow joins on regions that are within a threshold (instead of requiring overlap) [\#1473](https://github.com/bigdatagenomics/adam/issues/1473)
 - FeatureRDD.toCoverage throws NullPointerException when there is no coverage information [\#1471](https://github.com/bigdatagenomics/adam/issues/1471)
 - Add quality score binner [\#1462](https://github.com/bigdatagenomics/adam/issues/1462)
 - Splittable compression and FASTQ [\#1457](https://github.com/bigdatagenomics/adam/issues/1457)
 - Don't convert .{different-type}.adam in loadAlignments and loadFragments [\#1456](https://github.com/bigdatagenomics/adam/issues/1456)
 - New primitives for adam-core [\#1454](https://github.com/bigdatagenomics/adam/issues/1454)
 - Port over code for populating SequenceDictionaries from .dict files [\#1449](https://github.com/bigdatagenomics/adam/issues/1449)
 - Ignore failed push to Coveralls during CI builds [\#1444](https://github.com/bigdatagenomics/adam/issues/1444)
 - No asSingleFile parameter for saveAsFasta in NucleotideContigFragmentRDD [\#1438](https://github.com/bigdatagenomics/adam/issues/1438)
 - shufflejoin and ArrayIndexOutOfBoundsException [\#1436](https://github.com/bigdatagenomics/adam/issues/1436)
 - Document using ADAM snapshot [\#1432](https://github.com/bigdatagenomics/adam/issues/1432)
 - Improve metrics coverage across ADAMContext load methods [\#1428](https://github.com/bigdatagenomics/adam/issues/1428)
 - loadReferenceFile missing from Java API [\#1421](https://github.com/bigdatagenomics/adam/issues/1421)
 - loadCoverage missing from Java API [\#1420](https://github.com/bigdatagenomics/adam/issues/1420)
 - Question: How to get paired-end alignemntRecord like RDD[AlignmentRecord, AlignmentRecordRDD]? [\#1419](https://github.com/bigdatagenomics/adam/issues/1419)
 - Clean up possibly unused methods in Projection [\#1417](https://github.com/bigdatagenomics/adam/issues/1417)
 - Problem loading SNPeff annotated VCF [\#1390](https://github.com/bigdatagenomics/adam/issues/1390)
 - RecordGroupDictionary should support `isEmpty` [\#1380](https://github.com/bigdatagenomics/adam/issues/1380)
 - Get rid of mutable collection transformations in ShuffleRegionJoin [\#1379](https://github.com/bigdatagenomics/adam/issues/1379)
 - Add tab5/6 as native output format for AlignmentRecordRDD [\#1377](https://github.com/bigdatagenomics/adam/issues/1377)
 - ValidationStringency in MDTagging should apply to reads on unknown references [\#1365](https://github.com/bigdatagenomics/adam/issues/1365)
 - Assembly final name doesn't include spark2 for Spark 2.x builds [\#1361](https://github.com/bigdatagenomics/adam/issues/1361)
 - Merge reads2fragments and fragments2reads into a single CLI [\#1359](https://github.com/bigdatagenomics/adam/issues/1359)
 - Investigate failures to load ExAC.0.3.GRCh38.vcf variants [\#1351](https://github.com/bigdatagenomics/adam/issues/1351)
 - adam-shell does not allow additional jars via Spark jars argument [\#1349](https://github.com/bigdatagenomics/adam/issues/1349)
 - Loading GZipped VCF returns an empty RDD [\#1333](https://github.com/bigdatagenomics/adam/issues/1333)
 - Bump Spark 2 build to Spark 2.1.0 [\#1330](https://github.com/bigdatagenomics/adam/issues/1330)
 - Rename Transform command TransformAlignments or similar [\#1328](https://github.com/bigdatagenomics/adam/issues/1328)
 - Replace ADAM2Vcf and Vcf2ADAM commands with TransformGenotypes and TransformVariants [\#1327](https://github.com/bigdatagenomics/adam/issues/1327)
 - FeatureRDD instantiation tries to cache the RDD [\#1321](https://github.com/bigdatagenomics/adam/issues/1321)
 - Repository for Pipe API wrappers for bioinformatics tools [\#1314](https://github.com/bigdatagenomics/adam/issues/1314)
 - Trying to get Spark pipeline working with slightly out of date code. [\#1313](https://github.com/bigdatagenomics/adam/issues/1313)
 - Support for gVCF merging and genotyping (e.g. CombineGVCFs and GenotypeGVCFs) [\#1312](https://github.com/bigdatagenomics/adam/issues/1312)
 - Support for read alignment and variant calling in Adam? (e.g. BWA + Freebayes) [\#1311](https://github.com/bigdatagenomics/adam/issues/1311)
 - Don't include log4j.properties in published JAR [\#1300](https://github.com/bigdatagenomics/adam/issues/1300)
 - Removing ProgramRecords info when saving data to sam/bam? [\#1257](https://github.com/bigdatagenomics/adam/issues/1257)
 - ADAM on Slurm/LSF [\#1229](https://github.com/bigdatagenomics/adam/issues/1229)
 - Maintaining sorted/partitioned knowledge [\#1216](https://github.com/bigdatagenomics/adam/issues/1216)
 - Evaluate bdg-convert external conversion library proposal [\#1197](https://github.com/bigdatagenomics/adam/issues/1197)
 - Port AMPCamp Tutorial over [\#1174](https://github.com/bigdatagenomics/adam/issues/1174)
 - Top level WrappedRDD or similar abstraction [\#1173](https://github.com/bigdatagenomics/adam/issues/1173)
 - GFF3 formatted features written as single file must include gff-version pragma [\#1169](https://github.com/bigdatagenomics/adam/issues/1169)
 - Can probably eliminate sort in RealignIndels [\#1137](https://github.com/bigdatagenomics/adam/issues/1137)
 - Load SV type info field - need for allele uniquness [\#1134](https://github.com/bigdatagenomics/adam/issues/1134)
 - BroadcastRegionJoin is not a broadcast join [\#1110](https://github.com/bigdatagenomics/adam/issues/1110)
 - AlignmentRecordRDD does not extend GenomicRDD per javac [\#1092](https://github.com/bigdatagenomics/adam/issues/1092)
 - Add generic ReferenceRegion pushdown for parquet files [\#1047](https://github.com/bigdatagenomics/adam/issues/1047)
 - Use of dataset api in ADAM [\#1018](https://github.com/bigdatagenomics/adam/issues/1018)
 - Difference running markdups with and without projection [\#1014](https://github.com/bigdatagenomics/adam/issues/1014)
 - ADAM to BAM conversion fails using relative path [\#1012](https://github.com/bigdatagenomics/adam/issues/1012)
 - Refactor SequenceDictionary to use Contig instead of SequenceRecord [\#997](https://github.com/bigdatagenomics/adam/issues/997)
 - NoSuchMethodError due to kryo minor-version mismatch [\#955](https://github.com/bigdatagenomics/adam/issues/955)
 - Autogen field names in projection package [\#941](https://github.com/bigdatagenomics/adam/issues/941)
 - Future of schemas in bdg-formats [\#925](https://github.com/bigdatagenomics/adam/issues/925)
 - genotypeType for genotypes with multiple OtherAlt alleles? [\#897](https://github.com/bigdatagenomics/adam/issues/897)
 - How to filter genotype RDD with FeatureRDD [\#890](https://github.com/bigdatagenomics/adam/issues/890)
 - How to convert genotype DataFrame to VariantContext DataFrame / RDD [\#886](https://github.com/bigdatagenomics/adam/issues/886)
 - R language package for Adam [\#882](https://github.com/bigdatagenomics/adam/issues/882)
 - How to count genotypes with a 10 node Spark/Adam cluster faster than with BCFTools on a single machine? [\#879](https://github.com/bigdatagenomics/adam/issues/879)
 - Ensure Java API is up-to-date with Scala API [\#855](https://github.com/bigdatagenomics/adam/issues/855)
 - BroadcastRegionJoin fails with unmapped reads [\#821](https://github.com/bigdatagenomics/adam/issues/821)
 - Resolve Fragment vs. SingleReadBucket [\#789](https://github.com/bigdatagenomics/adam/issues/789)
 - Updating/Publishing the docs/ directory [\#774](https://github.com/bigdatagenomics/adam/issues/774)
 - Next on empty iterator in BroadcastRegionJoin [\#661](https://github.com/bigdatagenomics/adam/issues/661)
 - Cleanup code smell in sort work balancing code [\#635](https://github.com/bigdatagenomics/adam/issues/635)
 - Provide low-impact alternative to `transform -repartition` for reducing partition size [\#594](https://github.com/bigdatagenomics/adam/issues/594)
 - Create an ADAM Python API [\#538](https://github.com/bigdatagenomics/adam/issues/538)
 - Migrate serialization libraries out of ADAM core [\#448](https://github.com/bigdatagenomics/adam/issues/448)
 - Create standardized, interpretable exceptions for error reporting [\#420](https://github.com/bigdatagenomics/adam/issues/420)
 - Build info/version info inside ADAM-generated files [\#188](https://github.com/bigdatagenomics/adam/issues/188)

**Merged and closed pull requests:**

 - [ADAM-1854] Add requirements.txt file for RTD. [\#1856](https://github.com/bigdatagenomics/adam/pull/1856) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1783] Resolve check issues that block pushing to CRAN. [\#1849](https://github.com/bigdatagenomics/adam/pull/1849) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1847] Update ADAM scripts to support self-contained pip install. [\#1848](https://github.com/bigdatagenomics/adam/pull/1848) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1845] Only build and publish scaladocs in publish-scaladoc.sh. [\#1846](https://github.com/bigdatagenomics/adam/pull/1846) ([heuermh](https://github.com/heuermh))
 - [ADAM-1843] Install sources before calling scala:doc in publish scaladoc [\#1844](https://github.com/bigdatagenomics/adam/pull/1844) ([fnothaft](https://github.com/fnothaft))
 - Remove python and R profiles from release script [\#1842](https://github.com/bigdatagenomics/adam/pull/1842) ([heuermh](https://github.com/heuermh))
 - [ADAM-1817] Bump to Hadoop-BAM 7.9.1. [\#1841](https://github.com/bigdatagenomics/adam/pull/1841) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1838] Make populating variant.annotation field in Genotype configurable [\#1839](https://github.com/bigdatagenomics/adam/pull/1839) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1834] Add proper extensions for SAM/BAM/CRAM output formats. [\#1835](https://github.com/bigdatagenomics/adam/pull/1835) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1822] Misc docs cleanup [\#1827](https://github.com/bigdatagenomics/adam/pull/1827) ([fnothaft](https://github.com/fnothaft))
 - Added missing __init__.py for fulltoc. [\#1824](https://github.com/bigdatagenomics/adam/pull/1824) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1821] Add missing fulltoc for Sphinx documentation. [\#1823](https://github.com/bigdatagenomics/adam/pull/1823) ([fnothaft](https://github.com/fnothaft))
 - Fix link to documentation [\#1820](https://github.com/bigdatagenomics/adam/pull/1820) ([nzachow](https://github.com/nzachow))
 - [ADAM-1634] Add algorithm benchmarks to documentation. [\#1818](https://github.com/bigdatagenomics/adam/pull/1818) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1813] Delegate right outer shuffle region join to left OSRJ implementation. [\#1814](https://github.com/bigdatagenomics/adam/pull/1814) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1807] Check for empty partition when running a piped command. [\#1812](https://github.com/bigdatagenomics/adam/pull/1812) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1803] Refactor GenomicRDD.writeTextRdd to util.TextRddWriter. [\#1809](https://github.com/bigdatagenomics/adam/pull/1809) ([heuermh](https://github.com/heuermh))
 - Added Filter error when file loaded does not match schema [\#1805](https://github.com/bigdatagenomics/adam/pull/1805) ([akmorrow13](https://github.com/akmorrow13))
 - changed num_jars count [\#1802](https://github.com/bigdatagenomics/adam/pull/1802) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1795] Map -DskipTests=true to exec.skip for Python and R tests. [\#1800](https://github.com/bigdatagenomics/adam/pull/1800) ([heuermh](https://github.com/heuermh))
 - [ADAM-1672] Use working directory for R devtools::document(). [\#1798](https://github.com/bigdatagenomics/adam/pull/1798) ([heuermh](https://github.com/heuermh))
 - [ADAM-1789] Move scala-lang to provided scope. [\#1790](https://github.com/bigdatagenomics/adam/pull/1790) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1784] loadIndexedBam should pass the raw globbed path to Hadoop-BAM [\#1785](https://github.com/bigdatagenomics/adam/pull/1785) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1664] Add SUPPORT.md file to complement CONTRIBUTING.md. [\#1781](https://github.com/bigdatagenomics/adam/pull/1781) ([heuermh](https://github.com/heuermh))
 - [ADAM-1779] Adding code of contact adapted from the Contributor Convenant, version 1.4. [\#1780](https://github.com/bigdatagenomics/adam/pull/1780) ([heuermh](https://github.com/heuermh))
 - [ADAM-1661] Add file storage benchmarks. [\#1772](https://github.com/bigdatagenomics/adam/pull/1772) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1770] Genotype should only store core variant fields. [\#1771](https://github.com/bigdatagenomics/adam/pull/1771) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1768] Add InFormatter for unpaired FASTQ. [\#1769](https://github.com/bigdatagenomics/adam/pull/1769) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1643] Add S3 access documentation. [\#1767](https://github.com/bigdatagenomics/adam/pull/1767) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1763] Apply absolute value to destination partition in ModPartitioner [\#1766](https://github.com/bigdatagenomics/adam/pull/1766) ([fnothaft](https://github.com/fnothaft))
 - Add R and Python into distribution artifacts [\#1765](https://github.com/bigdatagenomics/adam/pull/1765) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1655] Move R package to bdgenomics.adam. [\#1764](https://github.com/bigdatagenomics/adam/pull/1764) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1753] Only emit realignment targets for reads containing a single INDEL [\#1756](https://github.com/bigdatagenomics/adam/pull/1756) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1715] Support validation stringency in Python/R. [\#1755](https://github.com/bigdatagenomics/adam/pull/1755) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1680] Eliminate non-determinism in the ShuffleRegionJoin. [\#1754](https://github.com/bigdatagenomics/adam/pull/1754) ([fnothaft](https://github.com/fnothaft))
 - update to _replaceRdd with tests [\#1749](https://github.com/bigdatagenomics/adam/pull/1749) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1747] Fixed subtract bug and tests [\#1748](https://github.com/bigdatagenomics/adam/pull/1748) ([devin-petersohn](https://github.com/devin-petersohn))
 - [ADAM-1745] Adding LeftOuterShuffleRegionJoinAndGroupByLeft and tests [\#1746](https://github.com/bigdatagenomics/adam/pull/1746) ([devin-petersohn](https://github.com/devin-petersohn))
 - Enabled thresholding for joins and standardized regionFn [\#1741](https://github.com/bigdatagenomics/adam/pull/1741) ([devin-petersohn](https://github.com/devin-petersohn))
 - Making join return types consistent [\#1737](https://github.com/bigdatagenomics/adam/pull/1737) ([devin-petersohn](https://github.com/devin-petersohn))
 - Opening up permissions on GenericGenomicRDD [\#1736](https://github.com/bigdatagenomics/adam/pull/1736) ([devin-petersohn](https://github.com/devin-petersohn))
 - [ADAM-1716] Add adam- prefix to distribution module name. [\#1733](https://github.com/bigdatagenomics/adam/pull/1733) ([heuermh](https://github.com/heuermh))
 - [ADAM-1695] Check for illegal genotype index after splitting multi-allelic variants. [\#1725](https://github.com/bigdatagenomics/adam/pull/1725) ([heuermh](https://github.com/heuermh))
 - [ADAM-1517] Bump Parquet version in a manner compatible with Spark 2.2.x [\#1722](https://github.com/bigdatagenomics/adam/pull/1722) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1512] Support VCFs with +Inf/-Inf float values. [\#1721](https://github.com/bigdatagenomics/adam/pull/1721) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1709] Add ability to left normalize reads containing INDELs. [\#1711](https://github.com/bigdatagenomics/adam/pull/1711) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1691] Move bdgenomics.adam to use a namespace package. [\#1706](https://github.com/bigdatagenomics/adam/pull/1706) ([fnothaft](https://github.com/fnothaft))
 - moved bdgenomics.adam package to bdgenomics-adam [\#1705](https://github.com/bigdatagenomics/adam/pull/1705) ([akmorrow13](https://github.com/akmorrow13))
 - Misc cleanup needed for bigdatagenomics/cannoli#65 [\#1704](https://github.com/bigdatagenomics/adam/pull/1704) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1699] Make GenomicRDD.toXxx method names consistent. [\#1700](https://github.com/bigdatagenomics/adam/pull/1700) ([heuermh](https://github.com/heuermh))
 - [ADAM-1694] Add short readable descriptions for toString in subclasses of GenomicRDD. [\#1698](https://github.com/bigdatagenomics/adam/pull/1698) ([heuermh](https://github.com/heuermh))
 - [ADAM-1693] Add adam-shell friendly VariantContextRDD.saveAsVcf method. [\#1696](https://github.com/bigdatagenomics/adam/pull/1696) ([heuermh](https://github.com/heuermh))
 - [ADAM-1688] Add bdg-formats exclusion to org.hammerlab:genomic-loci dependency. [\#1690](https://github.com/bigdatagenomics/adam/pull/1690) ([heuermh](https://github.com/heuermh))
 - [ADAM-1679] Unmapped items should not get caught in requirement when sorting [\#1687](https://github.com/bigdatagenomics/adam/pull/1687) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1566] Merge VCF header lines with VCFHeaderLineCount.INTEGER correctly. [\#1685](https://github.com/bigdatagenomics/adam/pull/1685) ([heuermh](https://github.com/heuermh))
 - [ADAM-1682] Add variant quality field. [\#1684](https://github.com/bigdatagenomics/adam/pull/1684) ([fnothaft](https://github.com/fnothaft))
 - Remove adam- prefix from module directory names. [\#1681](https://github.com/bigdatagenomics/adam/pull/1681) ([heuermh](https://github.com/heuermh))
 - Update to hadoop-bam 7.9.0 and htsjdk 2.11.0. [\#1678](https://github.com/bigdatagenomics/adam/pull/1678) ([heuermh](https://github.com/heuermh))
 - [ADAM-1676] Add more finely grained validation for INFO/FORMAT fields. [\#1677](https://github.com/bigdatagenomics/adam/pull/1677) ([fnothaft](https://github.com/fnothaft))
 - Python API fixes for AlignmentRecordRDD [\#1675](https://github.com/bigdatagenomics/adam/pull/1675) ([akmorrow13](https://github.com/akmorrow13))
 - [ADAM-1673] Don't set PL to empty when no PL is attached to a gVCF record [\#1674](https://github.com/bigdatagenomics/adam/pull/1674) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1670] Add ability to selectively project VCF fields. [\#1671](https://github.com/bigdatagenomics/adam/pull/1671) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1663] Enable read groups with repeated names when unioning. [\#1665](https://github.com/bigdatagenomics/adam/pull/1665) ([fnothaft](https://github.com/fnothaft))
 - Maint 2.11 0.18.0 [\#1659](https://github.com/bigdatagenomics/adam/pull/1659) ([Douglas-H](https://github.com/Douglas-H))
 - [ADAM-1630] Overhauled docs introduction and added architecture section. [\#1653](https://github.com/bigdatagenomics/adam/pull/1653) ([fnothaft](https://github.com/fnothaft))
 - Add adamR script [\#1651](https://github.com/bigdatagenomics/adam/pull/1651) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1647] Fix bad JAR discovery grep in bin/pyadam. [\#1648](https://github.com/bigdatagenomics/adam/pull/1648) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1548] Generate reStructuredText from pandoc markdown. [\#1646](https://github.com/bigdatagenomics/adam/pull/1646) ([fnothaft](https://github.com/fnothaft))
 - Algorithms docs formatting [\#1645](https://github.com/bigdatagenomics/adam/pull/1645) ([gunjanbaid](https://github.com/gunjanbaid))
 - Cleaned up docs. [\#1642](https://github.com/bigdatagenomics/adam/pull/1642) ([gunjanbaid](https://github.com/gunjanbaid))
 - Making example code compatible with current ADAM build [\#1641](https://github.com/bigdatagenomics/adam/pull/1641) ([devin-petersohn](https://github.com/devin-petersohn))
 - Cleaning up formatting and spacing of docs. [\#1640](https://github.com/bigdatagenomics/adam/pull/1640) ([devin-petersohn](https://github.com/devin-petersohn))
 - added ExtractRegions [\#1637](https://github.com/bigdatagenomics/adam/pull/1637) ([antonkulaga](https://github.com/antonkulaga))
 - [ADAM-1635] Eliminate passing FASTQ splittable status via config. [\#1636](https://github.com/bigdatagenomics/adam/pull/1636) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1614] Add VariantContextRDD to R and Python APIs. [\#1628](https://github.com/bigdatagenomics/adam/pull/1628) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1615] Add transform and transmute APIs to Java, R, and Python [\#1627](https://github.com/bigdatagenomics/adam/pull/1627) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1625] Use explicit types for header lines [\#1626](https://github.com/bigdatagenomics/adam/pull/1626) ([heuermh](https://github.com/heuermh))
 - [ADAM-1623] Add ProcessingStep to adam-codegen. [\#1624](https://github.com/bigdatagenomics/adam/pull/1624) ([heuermh](https://github.com/heuermh))
 - [ADAM-1607] Update distribution assembly task to attach assembly überjar [\#1622](https://github.com/bigdatagenomics/adam/pull/1622) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1490] Add asSingleFile to saveAsFastq and related. [\#1621](https://github.com/bigdatagenomics/adam/pull/1621) ([heuermh](https://github.com/heuermh))
 - Update load method docs in Python and R. [\#1619](https://github.com/bigdatagenomics/adam/pull/1619) ([heuermh](https://github.com/heuermh))
 - [ADAM-1616] Resolve installation directory if scripts are symlinks. [\#1617](https://github.com/bigdatagenomics/adam/pull/1617) ([heuermh](https://github.com/heuermh))
 - [ADAM-1611] Extend pipe APIs to Java, Python, and R. [\#1613](https://github.com/bigdatagenomics/adam/pull/1613) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1610] Mark non-serializable field in TwoBitFile as transient. [\#1612](https://github.com/bigdatagenomics/adam/pull/1612) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1554] Support saving BGZF VCF output. [\#1608](https://github.com/bigdatagenomics/adam/pull/1608) ([fnothaft](https://github.com/fnothaft))
 - Adding examples of how to use joins in the real world [\#1605](https://github.com/bigdatagenomics/adam/pull/1605) ([devin-petersohn](https://github.com/devin-petersohn))
 - [ADAM-1599] Add explicit functions for updating GenomicRDD metadata. [\#1600](https://github.com/bigdatagenomics/adam/pull/1600) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1576] Allow translation between two different GenomicRDD types. [\#1598](https://github.com/bigdatagenomics/adam/pull/1598) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1444] Ignore failed push to Coveralls. [\#1595](https://github.com/bigdatagenomics/adam/pull/1595) ([fnothaft](https://github.com/fnothaft))
 - Testing, testing, 1... 2... 3... [\#1592](https://github.com/bigdatagenomics/adam/pull/1592) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1417] Removed unused Projection.apply method, add test for Filter. [\#1591](https://github.com/bigdatagenomics/adam/pull/1591) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1579] Add unit test coverage for BED12 format. [\#1587](https://github.com/bigdatagenomics/adam/pull/1587) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1585] Support additional Illumina FASTQ metadata. [\#1586](https://github.com/bigdatagenomics/adam/pull/1586) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1438] Add ability to save FASTA back as a single file. [\#1581](https://github.com/bigdatagenomics/adam/pull/1581) ([fnothaft](https://github.com/fnothaft))
 - Bump bdg-formats correctly to 0.11.1, not SNAPSHOT. [\#1577](https://github.com/bigdatagenomics/adam/pull/1577) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1573] Remove unused Unaligned trait. [\#1574](https://github.com/bigdatagenomics/adam/pull/1574) ([fnothaft](https://github.com/fnothaft))
 - Slurm deployment readme [\#1571](https://github.com/bigdatagenomics/adam/pull/1571) ([jpdna](https://github.com/jpdna))
 - [ADAM-1564] Read VCF header from stream in VCFOutFormatter. [\#1565](https://github.com/bigdatagenomics/adam/pull/1565) ([heuermh](https://github.com/heuermh))
 - [ADAM-1562] Index off by one for VCF genotype Number=A attributes. [\#1563](https://github.com/bigdatagenomics/adam/pull/1563) ([heuermh](https://github.com/heuermh))
 - [ADAM-1533] Set Theory [\#1561](https://github.com/bigdatagenomics/adam/pull/1561) ([devin-petersohn](https://github.com/devin-petersohn))
 - Freebayes FORMAT=<ID=AO,Number=A attribute throws ArrayIndexOutOfBoundsException [\#1560](https://github.com/bigdatagenomics/adam/pull/1560) ([heuermh](https://github.com/heuermh))
 - [ADAM-1551] Emit non-reference model genotype at called sites. [\#1559](https://github.com/bigdatagenomics/adam/pull/1559) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1449] Add loadSequenceDictionary to ADAM context. [\#1557](https://github.com/bigdatagenomics/adam/pull/1557) ([heuermh](https://github.com/heuermh))
 - [ADAM-1537] Rename o.b.adam.apis.java package to o.b.adam.api.java [\#1556](https://github.com/bigdatagenomics/adam/pull/1556) ([heuermh](https://github.com/heuermh))
 - [ADAM-1549] Make regions provided to filterByOverlappingRegions an Iterable. [\#1550](https://github.com/bigdatagenomics/adam/pull/1550) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-941] Automatically generate projection enums.  [\#1547](https://github.com/bigdatagenomics/adam/pull/1547) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1361] Fix misnamed ADAM überjar. [\#1546](https://github.com/bigdatagenomics/adam/pull/1546) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1257] Add program record support for alignment/fragment files. [\#1545](https://github.com/bigdatagenomics/adam/pull/1545) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1359] Merge `reads2fragments` and `fragments2reads` into `transformFragments` [\#1543](https://github.com/bigdatagenomics/adam/pull/1543) ([fnothaft](https://github.com/fnothaft))
 - Fix minor format mistakes (and typo) in docs [\#1542](https://github.com/bigdatagenomics/adam/pull/1542) ([kkaneda](https://github.com/kkaneda))
 - Add a simple unit test to SingleFastqInputFormat [\#1541](https://github.com/bigdatagenomics/adam/pull/1541) ([kkaneda](https://github.com/kkaneda))
 - Support locus predicate in Transform [\#1540](https://github.com/bigdatagenomics/adam/pull/1540) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1421] Add java API for `loadReferenceFile`. [\#1536](https://github.com/bigdatagenomics/adam/pull/1536) ([fnothaft](https://github.com/fnothaft))
 - Refactor Vcf2ADAM and ADAM2Vcf into TransformGenotypes and TransformVariants [\#1532](https://github.com/bigdatagenomics/adam/pull/1532) ([heuermh](https://github.com/heuermh))
 - [ADAM-1530] Support loading GO:query (S/CR/B)AMs as fragments. [\#1531](https://github.com/bigdatagenomics/adam/pull/1531) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1169] Write GFF header line pragma in single file mode. [\#1529](https://github.com/bigdatagenomics/adam/pull/1529) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1501] Compute coverage using Dataset API. [\#1528](https://github.com/bigdatagenomics/adam/pull/1528) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1497] Add union to GenomicRDD. [\#1526](https://github.com/bigdatagenomics/adam/pull/1526) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1486] Respect validation stringency if BAM header load fails. [\#1525](https://github.com/bigdatagenomics/adam/pull/1525) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1499] Enable reuse of broadcasted objects in region join. [\#1524](https://github.com/bigdatagenomics/adam/pull/1524) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1520] Bump to bdg-formats 0.11.0. [\#1523](https://github.com/bigdatagenomics/adam/pull/1523) ([fnothaft](https://github.com/fnothaft))
 - Adding fragment InFormatter for Bowtie tab5 format [\#1522](https://github.com/bigdatagenomics/adam/pull/1522) ([heuermh](https://github.com/heuermh))
 - [ADAM-1328] Rename `Transform` to `TransformAlignments`. [\#1521](https://github.com/bigdatagenomics/adam/pull/1521) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1517] Move to Parquet 1.8.2 in preparation for moving to Spark 2.2.0 [\#1518](https://github.com/bigdatagenomics/adam/pull/1518) ([fnothaft](https://github.com/fnothaft))
 - Fixed minor typos in README. [\#1516](https://github.com/bigdatagenomics/adam/pull/1516) ([gunjanbaid](https://github.com/gunjanbaid))
 - Making TreeRegionJoin consistent with ShuffleRegionJoin [\#1515](https://github.com/bigdatagenomics/adam/pull/1515) ([devin-petersohn](https://github.com/devin-petersohn))
 - Resolve #1508, #1509 for Pipe API [\#1511](https://github.com/bigdatagenomics/adam/pull/1511) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1502] Preserve contig ordering in TwoBitFile sequence dictionary. [\#1508](https://github.com/bigdatagenomics/adam/pull/1508) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1483] Remove collapse parameter from AlignmentRecordRDD.toCoverage [\#1493](https://github.com/bigdatagenomics/adam/pull/1493) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1377] Adding fragment InFormatter for Bowtie tab6 format [\#1491](https://github.com/bigdatagenomics/adam/pull/1491) ([heuermh](https://github.com/heuermh))
 - [ADAM-1488] Only increment BQSR min quality by 33 once. [\#1489](https://github.com/bigdatagenomics/adam/pull/1489) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1481] Refactor ADAMContext loadXxx methods for consistency [\#1487](https://github.com/bigdatagenomics/adam/pull/1487) ([heuermh](https://github.com/heuermh))
 - Add quality score binner [\#1485](https://github.com/bigdatagenomics/adam/pull/1485) ([fnothaft](https://github.com/fnothaft))
 - Clean up ReferenceRegion.scala and add thresholded overlap and covers [\#1484](https://github.com/bigdatagenomics/adam/pull/1484) ([devin-petersohn](https://github.com/devin-petersohn))
 - [ADAM-1456] Remove .{type}.adam file extension conversions in type-guessing methods. [\#1482](https://github.com/bigdatagenomics/adam/pull/1482) ([heuermh](https://github.com/heuermh))
 - [ADAM-1480] Add switch to disable the fast concat method. [\#1479](https://github.com/bigdatagenomics/adam/pull/1479) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1476] Treat `.` ALT allele as symbolic non-ref. [\#1477](https://github.com/bigdatagenomics/adam/pull/1477) ([fnothaft](https://github.com/fnothaft))
 - Adding require for Coverage Conversion and related tests [\#1472](https://github.com/bigdatagenomics/adam/pull/1472) ([devin-petersohn](https://github.com/devin-petersohn))
 - Add cache argument to loadFeatures, additional Feature timers [\#1427](https://github.com/bigdatagenomics/adam/pull/1427) ([heuermh](https://github.com/heuermh))
 - [ADAM-882] R API [\#1397](https://github.com/bigdatagenomics/adam/pull/1397) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1018] Add support for Spark SQL Datasets. [\#1391](https://github.com/bigdatagenomics/adam/pull/1391) ([fnothaft](https://github.com/fnothaft))
 - WIP Python API [\#1387](https://github.com/bigdatagenomics/adam/pull/1387) ([fnothaft](https://github.com/fnothaft))
 - [ADAM-1365] Apply validation stringency to reads on missing contigs when MD tagging [\#1366](https://github.com/bigdatagenomics/adam/pull/1366) ([fnothaft](https://github.com/fnothaft))
 - Update dependency and plugin versions [\#1360](https://github.com/bigdatagenomics/adam/pull/1360) ([heuermh](https://github.com/heuermh))
 - [ADAM-1330] Move to Spark 2.1.0. [\#1332](https://github.com/bigdatagenomics/adam/pull/1332) ([fnothaft](https://github.com/fnothaft))
 - Efficient Joins and (re)Partitioning [\#1324](https://github.com/bigdatagenomics/adam/pull/1324) ([devin-petersohn](https://github.com/devin-petersohn))


### Version 0.22.0 ###

**Closed issues:**

- Realign all reads at target site, not just reads with no mismatches [\#1469](https://github.com/bigdatagenomics/adam/issues/1469)
- Parallel file merger fails if the output file is smaller than the HDFS block size [\#1467](https://github.com/bigdatagenomics/adam/issues/1467)
- Add new realigner arguments to docs [\#1465](https://github.com/bigdatagenomics/adam/issues/1465)
- Recalibrate method misspelled as recalibateBaseQualities [\#1463](https://github.com/bigdatagenomics/adam/issues/1463)
- FASTQ may try to split GZIPed files [\#1459](https://github.com/bigdatagenomics/adam/issues/1459)
- Update to Hadoop-BAM 7.8.0 [\#1455](https://github.com/bigdatagenomics/adam/issues/1455)
- Publish Markdown and Scaladoc to the interwebs [\#1453](https://github.com/bigdatagenomics/adam/issues/1453)
- Make VariantContextConverter public [\#1451](https://github.com/bigdatagenomics/adam/issues/1451)
- Apply method in FragmentRDD is package private [\#1445](https://github.com/bigdatagenomics/adam/issues/1445)
- Thread pool will block inside of pipe command for streams too large to buffer [\#1442](https://github.com/bigdatagenomics/adam/issues/1442)
- FeatureRDD.apply() does not allow addition of other parameters with defaults in the case class [\#1439](https://github.com/bigdatagenomics/adam/issues/1439)
- Question : Why the number of paired sequence in adam-0.21.0 less than adam-0.19.0? [\#1424](https://github.com/bigdatagenomics/adam/issues/1424)
- loadCoverage missing from Java API [\#1420](https://github.com/bigdatagenomics/adam/issues/1420)
- Estimate contig lengths in SequenceDictionary for BED, GFF3, GTF, and NarrowPeak feature formats [\#1410](https://github.com/bigdatagenomics/adam/issues/1410)
- loadIntervalList FeatureRDD has empty SequenceDictionary [\#1409](https://github.com/bigdatagenomics/adam/issues/1409)
- problem using transform command [\#1406](https://github.com/bigdatagenomics/adam/issues/1406)
- Add coveralls [\#1403](https://github.com/bigdatagenomics/adam/issues/1403)
- INDEL realigner binary search conditional is flipped [\#1402](https://github.com/bigdatagenomics/adam/issues/1402)
- Delete adam-scripts/R [\#1398](https://github.com/bigdatagenomics/adam/issues/1398)
- Data missing when transfroming FASTQ to Adam [\#1393](https://github.com/bigdatagenomics/adam/issues/1393)
- java.io.FileNotFoundException when file exists [\#1385](https://github.com/bigdatagenomics/adam/issues/1385)
- Off-by-1 error in FASTQ InputFormat start positioning code [\#1383](https://github.com/bigdatagenomics/adam/issues/1383)
- Set the wrong value for end for symbolic alts [\#1381](https://github.com/bigdatagenomics/adam/issues/1381)
- RecordGroupDictionary should support `isEmpty` [\#1380](https://github.com/bigdatagenomics/adam/issues/1380)
- Add pipe API in and out formatters for Features [\#1374](https://github.com/bigdatagenomics/adam/issues/1374)
- Increase visibility for SupportedHeaderLines.allHeaderLines [\#1372](https://github.com/bigdatagenomics/adam/issues/1372)
- Bits of VariantContextConverter don't get ValidationStringencied [\#1371](https://github.com/bigdatagenomics/adam/issues/1371)
- Add Markdown docs for Pipe API [\#1368](https://github.com/bigdatagenomics/adam/issues/1368)
- Array[Consensus] not registered [\#1367](https://github.com/bigdatagenomics/adam/issues/1367)
- ValidationStringency in MDTagging should apply to reads on unknown references [\#1365](https://github.com/bigdatagenomics/adam/issues/1365)
- When doing a release, the SNAPSHOT should bump by 0.1.0, not 0.0.1 [\#1364](https://github.com/bigdatagenomics/adam/issues/1364)
- FromKnowns consensus generator fails if no reads overlap a consensus [\#1362](https://github.com/bigdatagenomics/adam/issues/1362)
- Performance tune-up in BQSR [\#1358](https://github.com/bigdatagenomics/adam/issues/1358)
- Increase visibility for ADAMContext.sc and/or getFs... methods [\#1356](https://github.com/bigdatagenomics/adam/issues/1356)
- Pipe API formatters need to be public [\#1354](https://github.com/bigdatagenomics/adam/issues/1354)
- Version 0.21.0: VariantContextConverter fails for 1000G VCF data [\#1353](https://github.com/bigdatagenomics/adam/issues/1353)
- ConsensusModel's can't really be instantiated [\#1352](https://github.com/bigdatagenomics/adam/issues/1352)
- Runtime conflicts in transitive versions of Guava dependency [\#1350](https://github.com/bigdatagenomics/adam/issues/1350)
- Transcript Effects ignored if more than 1 [\#1347](https://github.com/bigdatagenomics/adam/issues/1347)
- Remove "fork" tag from releases [\#1344](https://github.com/bigdatagenomics/adam/issues/1344)
- Refactor isSorted boolean parameters to sorted [\#1341](https://github.com/bigdatagenomics/adam/issues/1341)
- Loading GZipped VCF returns an empty RDD [\#1333](https://github.com/bigdatagenomics/adam/issues/1333)
- Follow up on error messages in build scripts [\#1331](https://github.com/bigdatagenomics/adam/issues/1331)
- Bump Spark 2 build to Spark 2.1.0 [\#1330](https://github.com/bigdatagenomics/adam/issues/1330)
- FeatureRDD instantiation tries to cache the RDD [\#1321](https://github.com/bigdatagenomics/adam/issues/1321)
- Load queryname sorted BAMs as Fragments [\#1303](https://github.com/bigdatagenomics/adam/issues/1303)
- Run Duplicate Marking on Fragments [\#1302](https://github.com/bigdatagenomics/adam/issues/1302)
- GenomicRDD.pipe may hang on failure error codes [\#1282](https://github.com/bigdatagenomics/adam/issues/1282)
- IllegalArgumentException Wrong FS for vcf_head files on HDFS [\#1272](https://github.com/bigdatagenomics/adam/issues/1272)
- java.io.NotSerializableException: org.bdgenomics.formats.avro.AlignmentRecord [\#1240](https://github.com/bigdatagenomics/adam/issues/1240)
- Investigate sorted join in dataset api [\#1223](https://github.com/bigdatagenomics/adam/issues/1223)
- Support looser validation stringency for loading some VCF Integer fields [\#1213](https://github.com/bigdatagenomics/adam/issues/1213)
- Add new feature-overlap command to demonstrate new region joins [\#1194](https://github.com/bigdatagenomics/adam/issues/1194)
- What should our API at the command line look like? [\#1178](https://github.com/bigdatagenomics/adam/issues/1178)
- Split apart partition and join in ShuffleRegionJoin [\#1175](https://github.com/bigdatagenomics/adam/issues/1175)
- Merging files should be multithreaded [\#1164](https://github.com/bigdatagenomics/adam/issues/1164)
- File _rgdict.avro does not exist [\#1150](https://github.com/bigdatagenomics/adam/issues/1150)
- how to collect the .adam files from Spark cluster multiple nodes and some questions about avocado [\#1140](https://github.com/bigdatagenomics/adam/issues/1140)
- JFYI: tiny forked adam-core "0.20.0" release [\#1139](https://github.com/bigdatagenomics/adam/issues/1139)
- Samtools (htslib) integration testing [\#1120](https://github.com/bigdatagenomics/adam/issues/1120)
- AlignmentRecordRDD does not extend GenomicRDD per javac [\#1092](https://github.com/bigdatagenomics/adam/issues/1092)
- Release ADAM version 0.21.0 [\#1088](https://github.com/bigdatagenomics/adam/issues/1088)
- Difference running markdups with and without projection [\#1014](https://github.com/bigdatagenomics/adam/issues/1014)
- ADAM to BAM conversion fails using relative path [\#1012](https://github.com/bigdatagenomics/adam/issues/1012)
- Refactor SequenceDictionary to use Contig instead of SequenceRecord [\#997](https://github.com/bigdatagenomics/adam/issues/997)
- Customize adam-main cli from configuration file [\#918](https://github.com/bigdatagenomics/adam/issues/918)
- genotypeType for genotypes with multiple OtherAlt alleles? [\#897](https://github.com/bigdatagenomics/adam/issues/897)
- How to convert genotype DataFrame to VariantContext DataFrame / RDD [\#886](https://github.com/bigdatagenomics/adam/issues/886)
- Ensure Java API is up-to-date with Scala API [\#855](https://github.com/bigdatagenomics/adam/issues/855)
- Improve parallelism during FASTA output [\#842](https://github.com/bigdatagenomics/adam/issues/842)
- Explicitly validate user args passed to transform enhancement [\#841](https://github.com/bigdatagenomics/adam/issues/841)
- BroadcastRegionJoin fails with unmapped reads [\#821](https://github.com/bigdatagenomics/adam/issues/821)
- Resolve Fragment vs. SingleReadBucket [\#789](https://github.com/bigdatagenomics/adam/issues/789)
- Add profile for skipping test compilation/resolution [\#713](https://github.com/bigdatagenomics/adam/issues/713)
- Next on empty iterator in BroadcastRegionJoin [\#661](https://github.com/bigdatagenomics/adam/issues/661)
- Cleanup code smell in sort work balancing code [\#635](https://github.com/bigdatagenomics/adam/issues/635)
- Remove reliance on MD tags [\#622](https://github.com/bigdatagenomics/adam/issues/622)
- Provide low-impact alternative to `transform -repartition` for reducing partition size [\#594](https://github.com/bigdatagenomics/adam/issues/594)
- Clean up Rich records [\#577](https://github.com/bigdatagenomics/adam/issues/577)
- Create standardized, interpretable exceptions for error reporting [\#420](https://github.com/bigdatagenomics/adam/issues/420)
- Create ADAM Benchmarking suite [\#120](https://github.com/bigdatagenomics/adam/issues/120)

**Merged and closed pull requests:**

- [ADAM-1469] Don't filter on whether reads have mismatches during realignment [\#1470](https://github.com/bigdatagenomics/adam/pull/1470) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1467] Skip `concat` call if there is only one shard. [\#1468](https://github.com/bigdatagenomics/adam/pull/1468) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1465] Updating realigner CLI docs. [\#1466](https://github.com/bigdatagenomics/adam/pull/1466) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1463] Rename recalibateBaseQualities method as recalibrateBaseQualities [\#1464](https://github.com/bigdatagenomics/adam/pull/1464) ([heuermh](https://github.com/heuermh))
- [ADAM-1453] Add hooks to publish ADAM docs from CI flow. [\#1461](https://github.com/bigdatagenomics/adam/pull/1461) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1459] Don't split FASTQ when compressed. [\#1459](https://github.com/bigdatagenomics/adam/pull/1459) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1451] Make VariantContextConverter class and convert methods public [\#1452](https://github.com/bigdatagenomics/adam/pull/1452) ([fnothaft](https://github.com/fnothaft))
- Moving API overview from building apps doc to new source file. [\#1450](https://github.com/bigdatagenomics/adam/pull/1450) ([heuermh](https://github.com/heuermh))
- [ADAM-1424] Adding test for reads dropped in 0.21.0. [\#1448](https://github.com/bigdatagenomics/adam/pull/1448) ([heuermh](https://github.com/heuermh))
- [ADAM-1439] Add inferSequenceDictionary ctr to FeatureRDD. [\#1447](https://github.com/bigdatagenomics/adam/pull/1447) ([heuermh](https://github.com/heuermh))
- [ADAM-1445] Make apply method for FragmentRDD public. [\#1446](https://github.com/bigdatagenomics/adam/pull/1446) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1442] Fix thread pool deadlock in GenomicRDD.pipe [\#1443](https://github.com/bigdatagenomics/adam/pull/1443) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1164] Add parallel file merger. [\#1441](https://github.com/bigdatagenomics/adam/pull/1441) ([fnothaft](https://github.com/fnothaft))
- Dependency version bump + BroadcastRegionJoin fix [\#1440](https://github.com/bigdatagenomics/adam/pull/1440) ([fnothaft](https://github.com/fnothaft))
- added JavaApi for loadCoverage [\#1437](https://github.com/bigdatagenomics/adam/pull/1437) ([akmorrow13](https://github.com/akmorrow13))
- Update versions, etc. in build docs [\#1435](https://github.com/bigdatagenomics/adam/pull/1435) ([heuermh](https://github.com/heuermh))
- Add test sample(verify number of reads in loadAlignments function) and ADAM SNAPSHOT document [\#1433](https://github.com/bigdatagenomics/adam/pull/1433) ([xubo245](https://github.com/xubo245))
- Add cache argument to loadFeatures, additional Feature timers [\#1427](https://github.com/bigdatagenomics/adam/pull/1427) ([heuermh](https://github.com/heuermh))
- feat: speed up 2bit file extract [\#1426](https://github.com/bigdatagenomics/adam/pull/1426) ([Blaok](https://github.com/Blaok))
- BQSR refactor for perf improvements [\#1423](https://github.com/bigdatagenomics/adam/pull/1423) ([fnothaft](https://github.com/fnothaft))
- Add ADAMContext/GenomicRDD/pipe docs [\#1422](https://github.com/bigdatagenomics/adam/pull/1422) ([fnothaft](https://github.com/fnothaft))
- INDEL realigner cleanup [\#1412](https://github.com/bigdatagenomics/adam/pull/1412) ([fnothaft](https://github.com/fnothaft))
- Estimate contig lengths in SequenceDictionary for BED, GFF3, GTF, and NarrowPeak feature formats [\#1411](https://github.com/bigdatagenomics/adam/pull/1411) ([heuermh](https://github.com/heuermh))
- Add coveralls badge to README.md. [\#1408](https://github.com/bigdatagenomics/adam/pull/1408) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1403] Push coverage reports to Coveralls. [\#1404](https://github.com/bigdatagenomics/adam/pull/1404) ([fnothaft](https://github.com/fnothaft))
- Added instrumentation timers around joins. [\#1401](https://github.com/bigdatagenomics/adam/pull/1401) ([fnothaft](https://github.com/fnothaft))
- Add Apache Spark version to --version text [\#1400](https://github.com/bigdatagenomics/adam/pull/1400) ([heuermh](https://github.com/heuermh))
- [ADAM-1398] Delete adam-scripts/R. [\#1399](https://github.com/bigdatagenomics/adam/pull/1399) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1383] Use gt instead of gteq in FASTQ input format line size checks [\#1396](https://github.com/bigdatagenomics/adam/pull/1396) ([fnothaft](https://github.com/fnothaft))
- Maint spark2 2.11 0.21.0 [\#1395](https://github.com/bigdatagenomics/adam/pull/1395) ([A-Tsai](https://github.com/A-Tsai))
- [ADAM-1393] fix missing reads when transforming fastq to adam [\#1394](https://github.com/bigdatagenomics/adam/pull/1394) ([A-Tsai](https://github.com/A-Tsai))
- [ADAM-1380] Adds isEmpty method to RecordGroupDictionary. [\#1392](https://github.com/bigdatagenomics/adam/pull/1392) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1381] Fix Variant end position. [\#1389](https://github.com/bigdatagenomics/adam/pull/1389) ([fnothaft](https://github.com/fnothaft))
- Make javac see that AlignmentRecordRDD extends GenomicRDD [\#1386](https://github.com/bigdatagenomics/adam/pull/1386) ([fnothaft](https://github.com/fnothaft))
- Added ShuffleRegionJoin usage docs [\#1384](https://github.com/bigdatagenomics/adam/pull/1384) ([devin-petersohn](https://github.com/devin-petersohn))
- Misc. INDEL realigner bugfixes [\#1382](https://github.com/bigdatagenomics/adam/pull/1382) ([fnothaft](https://github.com/fnothaft))
- Add pipe API in and out formatters for Features [\#1378](https://github.com/bigdatagenomics/adam/pull/1378) ([heuermh](https://github.com/heuermh))
- [ADAM-1356] Make ADAMContext.getFsAndFiles and related protected visibility [\#1376](https://github.com/bigdatagenomics/adam/pull/1376) ([heuermh](https://github.com/heuermh))
- [ADAM-1372] Increase visibility for DefaultHeaderLines.allHeaderLines [\#1375](https://github.com/bigdatagenomics/adam/pull/1375) ([heuermh](https://github.com/heuermh))
- [ADAM-1371] Wrap ADAM->htsjdk VariantContext conversion with validation stringency. [\#1373](https://github.com/bigdatagenomics/adam/pull/1373) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1367] Register Consensus array for serialization. [\#1369](https://github.com/bigdatagenomics/adam/pull/1369) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1365] Apply validation stringency to reads on missing contigs when MD tagging [\#1366](https://github.com/bigdatagenomics/adam/pull/1366) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1362] Fixing issue where FromKnowns consensus model fails if no reads hit a target. [\#1363](https://github.com/bigdatagenomics/adam/pull/1363) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1352] Clean up consensus model usage. [\#1357](https://github.com/bigdatagenomics/adam/pull/1357) ([fnothaft](https://github.com/fnothaft))
- Increase visibility for InFormatter case classes from package private to public [\#1355](https://github.com/bigdatagenomics/adam/pull/1355) ([heuermh](https://github.com/heuermh))
- Use htsjdk getAttributeAsList for VCF INFO ANN key [\#1348](https://github.com/bigdatagenomics/adam/pull/1348) ([heuermh](https://github.com/heuermh))
- Fixes parsing variant annotations for multi-allelic rows [\#1346](https://github.com/bigdatagenomics/adam/pull/1346) ([majkiw](https://github.com/majkiw))
- Sort pull requests by id [\#1345](https://github.com/bigdatagenomics/adam/pull/1345) ([heuermh](https://github.com/heuermh))
- HBase genotypes backend -revised [\#1335](https://github.com/bigdatagenomics/adam/pull/1335) ([jpdna](https://github.com/jpdna))
- [ADAM-1330] Move to Spark 2.1.0. [\#1332](https://github.com/bigdatagenomics/adam/pull/1332) ([fnothaft](https://github.com/fnothaft))
- Support deduping fragments [\#1309](https://github.com/bigdatagenomics/adam/pull/1309) ([fnothaft](https://github.com/fnothaft))
- [ADAM-1280] Silence CRAM logging in tests. [\#1294](https://github.com/bigdatagenomics/adam/pull/1294) ([fnothaft](https://github.com/fnothaft))
- Added test to try and repro #1282. [\#1292](https://github.com/bigdatagenomics/adam/pull/1292) ([fnothaft](https://github.com/fnothaft))

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
