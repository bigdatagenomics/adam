-- Register all the Parquet jars for this Pig job
-- To create these jar, checkout the parquet-pig and parquet-format projects from
-- github, run `mvn clean package` in both projects and update the locations of 
-- the jars below to match your environment.
register /workspace/parquet-mr/parquet-pig/target/parquet-pig-1.0.0-SNAPSHOT.jar;
register /workspace/parquet-mr/parquet-column/target/parquet-column-1.0.0-SNAPSHOT.jar;
register /workspace/parquet-mr/parquet-hadoop/target/parquet-hadoop-1.0.0-SNAPSHOT.jar;
register /workspace/parquet-format/target/parquet-format-1.0.0-SNAPSHOT.jar;

-- Load the read data from the Parquet file.
-- For now, you are required to pass in a schema to the loader. It, of course, must
-- match the ADAM record schema but can be a subset. Using a subset will actually
-- help improve performance since Parquet will materialize only the columns you want.
-- Future versions of Parquet will read the Parquet schema and convert it to 
-- a Pig schema automatically.

reads = LOAD 'overlapping.bam.adam1' 
		using parquet.pig.ParquetLoader('
			referenceName:chararray, 
			start:long, 
        		end:long,
        		readName:chararray,
        		sequence:chararray,
        		mapq:long,
			readUnmappedFlag:boolean, 
			duplicateReadFlag:boolean, 
			notPrimaryAlignmentFlag:boolean, 
			readFailedVendorQualityCheckFlag:boolean');

-- Filter out all reads that don't have a mapq of at least 30, are duplicates,
-- are not the primary alignment, are unmapped or failed the vendor quality checks

good_reads = filter reads by 
		sequence is not null 
        	and mapq >= 30
		and (duplicateReadFlag is null or not duplicateReadFlag)
		and (notPrimaryAlignmentFlag is null or not notPrimaryAlignmentFlag)
		and (readUnmappedFlag is null or not readUnmappedFlag)
		and (readFailedVendorQualityCheckFlag is null or not readFailedVendorQualityCheckFlag);

reads_groupby_reference = GROUP reads by referenceName;
total_reads_per_reference = foreach reads_groupby_reference GENERATE
	group as referenceName, COUNT(reads);

good_reads_groupby_reference = GROUP good_reads by referenceName;
good_reads_per_reference = foreach good_reads_groupby_reference GENERATE
	group as referenceName, COUNT(good_reads);

reads_join = JOIN total_reads_per_reference by referenceName,
		good_reads_per_reference by referenceName;

store reads_join into 'results';
