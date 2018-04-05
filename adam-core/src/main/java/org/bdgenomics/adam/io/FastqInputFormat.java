/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.io;

import htsjdk.samtools.util.BlockCompressedInputStream;
import java.io.BufferedInputStream;
import java.io.InputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec;

public abstract class FastqInputFormat extends FileInputFormat<Void, Text> {

    protected boolean splittable;

    /**
     * Checks to see if the file we are looking at is splittable.
     *
     * A file is splittable if it is:
     *
     * - Uncompressed.
     * - Compressed with the BGZFEnhancedGzipCodec _and_ the underlying stream is
     *   a BGZF stream. BGZFEnhancedGzipCodec looks for files with a .gz
     *   extension, which means that the codec may be selected if the file is a
     *   non-block GZIPed file, and thus is non-splittable. To validate this, we
     *   use HTSJDKs in-built mechanism for checking if a stream is a BGZF stream.
     * - Any other splittable codec (e.g., .bgz/BGZFCodec, .bz2/BZip2Codec) 
     *
     * @param context The job context to get the configuration from.
     * @param filename The path the input file is saved at.
     * @return Returns false if this file is compressed.
     */
    @Override protected boolean isSplitable(final JobContext context,
                                            final Path filename) {
        Configuration conf = context.getConfiguration();
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        if (codec == null) {
            splittable = true;
        } else if (codec instanceof BGZFEnhancedGzipCodec) {
            // BGZFEnhancedGzipCodec extends SplittableCompressionCodec, so this
            // should be unnecessary. but...
            //
            // as documented in the method javadoc, BGZFEnhancedGzipCodec matches
            // to files with a .gz extension, which can either be BGZF or GZIP
            // so we must actually look at the stream to determine if the file
            // is BGZF and splittable or GZIP and not-splittable
            
	    try(InputStream is = filename.getFileSystem(conf).open(filename)) {
		// htsjdk can only test a buffered input stream
		// throws an exception if the stream is unbuffered
		// why htsjdk doesn't make the function take a buffered input stream instead of
		// an input stream, i do not know
		splittable = BlockCompressedInputStream.isValidFile(new BufferedInputStream(is));
	    } catch (Exception e) {
                splittable = false;
	    }
        } else if (codec instanceof SplittableCompressionCodec) {
            splittable = true;
        } else {
            splittable = false;
        }

        return splittable;
    }
}
