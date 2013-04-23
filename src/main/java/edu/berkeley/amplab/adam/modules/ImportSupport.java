/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.amplab.adam.modules;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.kohsuke.args4j.Option;
import parquet.avro.AvroParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

/**
 * Class that contains options and methods that are common to all import classes
 */
abstract public class ImportSupport extends AdamModule {

    @Override
    abstract public String getModuleName();

    @Override
    abstract public String getModuleDescription();

    @Option(name = "-compress", usage = "Compress data? Default = true")
    public boolean compress = true;

    @Option(name = "-codec", usage = "Compression codec: GZIP (default), SNAPPY, LZO, UNCOMPRESSED")
    public String compressionCodec = CompressionCodecName.GZIP.toString();

    @Option(name = "-block_size",
            usage = "The column store block size. Default = 50MB")
    public int blockSize = 50 * 1024 * 1024;

    @Option(name = "-page_size",
            usage = "The column store page size. Default = 1MB")
    public int pageSize = 1 * 1024 * 1024;

    public Job setupJob() throws IOException {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJobName(getModuleName());
        job.setJarByClass(ImportSupport.class);
        return job;
    }

    public void setupOutputFormat(Job job, Schema schema, Path outputPath) {
        AvroParquetOutputFormat.setCompressOutput(job, compress);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.fromConf(compressionCodec));
        AvroParquetOutputFormat.setSchema(job, schema);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setBlockSize(job, blockSize);
        AvroParquetOutputFormat.setPageSize(job, pageSize);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
    }

    public static Path generateOutputPath(String inputPath) {
        return new Path(inputPath + ".adam1");
    }

}
