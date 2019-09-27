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

import java.io.EOFException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This class is a Hadoop reader for "interleaved fastq" -- that is,
 * fastq with paired reads in the same file, interleaved, rather than
 * in two separate files. This makes it much easier to Hadoopily slice
 * up a single file and feed the slices into an aligner.
 * The format is the same as fastq, but records are expected to alternate
 * between /1 and /2. As a precondition, we assume that the interleaved
 * FASTQ files are always uncompressed; if the files are compressed, they
 * cannot be split, and thus there is no reason to use the interleaved
 * format.
 *
 * This reader is based on the FastqInputFormat that's part of Hadoop-BAM,
 * found at https://github.com/HadoopGenomics/Hadoop-BAM/blob/master/src/main/java/org/seqdoop/hadoop_bam/FastqInputFormat.java
 */
public final class InterleavedFastqInputFormat extends FastqInputFormat {

    /**
     * A record reader for the interleaved FASTQ format.
     *
     * Reads over an input file and parses interleaved FASTQ read pairs into
     * a single Text output. This is then fed into the FastqConverter, which
     * converts the single Text instance into two Alignments.
     */
    private static class InterleavedFastqRecordReader extends FastqRecordReader {

        private final String firstReadSuffix = ".+([/ +_]1| 1:[YN]:[02468]+:[0-9ACTNG]+)$";
        private Pattern firstReadRegex;

        InterleavedFastqRecordReader(final Configuration conf,
                                     final FileSplit split) throws IOException {
            super(conf, split);
        }

        /**
         * Checks to see whether the buffer is positioned at a valid record.
         *
         * We are properly positioned if the buffer contains a read name (starts
         * with '@'), and this read name has a first-of-pair suffix (ends with
         * '/1').
         *
         * @param bufferLength The length of the line currently in the buffer.
         * @param buffer A buffer containing a peek at the first line in the current
         *   stream.
         * @return Returns true if the buffer contains the first line of a properly
         *   formatted pair of FASTQ records.
         */
        protected boolean checkBuffer(final int bufferLength, final Text buffer) {
            if (firstReadRegex == null) {
                firstReadRegex = Pattern.compile(firstReadSuffix);
            }

            if (bufferLength > 2 &&
                buffer.getBytes()[0] == '@') {
                
                // attempt to match regex
                Matcher bufferMatcher = firstReadRegex.matcher(buffer.toString());
                return bufferMatcher.matches();
            } else {
                return false;
            }
        }

        /**
         * Reads a read pair from the input split.
         *
         * @param value Text record to write input value into.
         * @return Returns whether this read was successful or not.
         *
         * @throws RuntimeException Throws exception if we hit an EOF in the
         *   middle of a read, or if we have a read that is incorrectly
         *   formatted (missing readname delimiters).
         */
        protected boolean next(final Text value) throws IOException {
            if (pos >= end)
                return false; // past end of slice
            try {
                Text readName1 = new Text();
                Text readName2 = new Text();
                value.clear();

                // first read of the pair
                if (!lowLevelFastqRead(readName1, value)) {
                    return false;
                }

                // second read of the pair
                return lowLevelFastqRead(readName2, value);

            } catch (EOFException e) {
                throw new RuntimeException("unexpected end of file in fastq record at " + makePositionMessage());
            }
        }
    }

    /**
     * Creates the new record reader that underlies this input format.
     *
     * @param genericSplit The split that the record reader should read.
     * @param context The Hadoop task context.
     * @return Returns the interleaved FASTQ record reader.
     */
    public RecordReader<Void, Text> createRecordReader(
            final InputSplit genericSplit,
            final TaskAttemptContext context) throws IOException, InterruptedException {
        context.setStatus(genericSplit.toString());

        // cast as per example in TextInputFormat
        return new InterleavedFastqRecordReader(context.getConfiguration(),
                                                (FileSplit) genericSplit);
    }
}
