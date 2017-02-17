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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This class is a Hadoop reader for single read fastq.
 *
 * This reader is based on the FastqInputFormat that's part of Hadoop-BAM,
 * found at https://github.com/HadoopGenomics/Hadoop-BAM/blob/master/src/main/java/org/seqdoop/hadoop_bam/FastqInputFormat.java
 */
public final class SingleFastqInputFormat extends FileInputFormat<Void, Text> {

    /**
     * A record reader for the standard FASTQ format.
     *
     * Reads over an input file and parses FASTQ read pairs into Text. This is
     * then fed into the FastqConverter, which converts the Text instance into
     * an AlignmentRecord.
     */
    private static class SingleFastqRecordReader extends FastqRecordReader {

        SingleFastqRecordReader(final Configuration conf,
                                final FileSplit split) throws IOException {
            super(conf, split);
        }

        /**
         * Checks to see whether the buffer is positioned at a valid record.
         *
         * We are properly positioned if the buffer contains a read name (starts
         * with '@'). We do not check for a read name suffix.
         *
         * @param bufferLength The length of the line currently in the buffer.
         * @param buffer A buffer containing a peek at the first line in the current
         *   stream.
         * @return Returns true if the buffer contains the first line of a properly
         *   formatted pair of FASTQ records.
         */
        protected boolean checkBuffer(final int bufferLength, final Text buffer) {
            return (bufferLength > 0 &&
                    buffer.getBytes()[0] == '@');
        }

        /**
         * Reads the next read from the input split.
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
                Text readName = new Text();
                value.clear();

                // first read of the pair
                return lowLevelFastqRead(readName, value);
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
        return new SingleFastqRecordReader(context.getConfiguration(), (FileSplit) genericSplit);
    }
}
