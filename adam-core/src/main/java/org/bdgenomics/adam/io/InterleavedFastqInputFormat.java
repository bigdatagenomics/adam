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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

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
 * found at http://sourceforge.net/p/hadoop-bam/code/ci/master/tree/src/fi/tkk/ics/hadoop/bam/FastqInputFormat.java
 *
 * @author Jeremy Elson (jelson@microsoft.com)
 * @date Feb 2014
 */
public class InterleavedFastqInputFormat extends FileInputFormat<Void,Text> {
    
    public static class InterleavedFastqRecordReader extends RecordReader<Void,Text> {
        /*
         * fastq format:
         * <fastq>  :=  <block>+
         * <block>  :=  @<seqname>\n<seq>\n+[<seqname>]\n<qual>\n
         * <seqname>  :=  [A-Za-z0-9_.:-]+
         * <seq>  :=  [A-Za-z\n\.~]+
         * <qual> :=  [!-~\n]+
         *
         * LP: this format is broken, no?  You can have multi-line sequence and quality strings,
         * and the quality encoding includes '@' in its valid character range.  So how should one
         * distinguish between \n@ as a record delimiter and and \n@ as part of a multi-line
         * quality string?
         *
         * For now I'm going to assume single-line sequences.  This works for our sequencing
         * application.  We'll see if someone complains in other applications.
         */
        
        // start:  first valid data index
        private long start;
        // end:  first index value beyond the slice, i.e. slice is in range [start,end)
        private long end;
        // pos: current position in file
        private long pos;
        // file:  the file being read
        private Path file;
        
        private LineReader lineReader;
        private InputStream inputStream;
        private Text currentValue;
        private byte[] newline = "\n".getBytes();

        // How long can a read get?
        private static final int MAX_LINE_LENGTH = 10000;

        public InterleavedFastqRecordReader(Configuration conf, FileSplit split) throws IOException {
            file = split.getPath();
            start = split.getStart();
            end = start + split.getLength();

            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream fileIn = fs.open(file);

            CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
            CompressionCodec        codec        = codecFactory.getCodec(file);

            if (codec == null) { // no codec.  Uncompressed file.
                positionAtFirstRecord(fileIn);
                inputStream = fileIn;
            } else { 
                // compressed file
                if (start != 0) {
                    throw new RuntimeException("Start position for compressed file is not 0! (found " + start + ")");
                }

                inputStream = codec.createInputStream(fileIn);
                end = Long.MAX_VALUE; // read until the end of the file
            }

            lineReader = new LineReader(inputStream);
        }

        /**
         * Position the input stream at the start of the first record.
         */
        private void positionAtFirstRecord(FSDataInputStream stream) throws IOException {
            Text buffer = new Text();

            if (true) { // (start > 0) // use start>0 to assume that files start with valid data
                // Advance to the start of the first record that ends with /1
                // We use a temporary LineReader to read lines until we find the
                // position of the right one.  We then seek the file to that position.
                stream.seek(start);
                LineReader reader = new LineReader(stream);

                int bytesRead = 0;
                do {
                    bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
                    int bufferLength = buffer.getLength();
                    if (bytesRead > 0 && (bufferLength <= 0 ||
                                          buffer.getBytes()[0] != '@' ||
                                          (bufferLength >= 2 && buffer.getBytes()[bufferLength - 2] != '/') ||
                                          (bufferLength >= 1 && buffer.getBytes()[bufferLength - 1] != '1'))) {
                        start += bytesRead;
                    } else {
                        // line starts with @.  Read two more and verify that it starts with a +
                        //
                        // If this isn't the start of a record, we want to backtrack to its end
                        long backtrackPosition = start + bytesRead;

                        bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
                        bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
                        if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+') {
                            break; // all good!
                        } else {
                            // backtrack to the end of the record we thought was the start.
                            start = backtrackPosition;
                            stream.seek(start);
                            reader = new LineReader(stream);
                        }
                    }
                } while (bytesRead > 0);

                stream.seek(start);
            }

            pos = start;
        }

        /**
         * Added to use mapreduce API.
         */
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {}

        /**
         * Added to use mapreduce API.
         */
        public Void getCurrentKey() {
            return null;
        }

        /**
         * Added to use mapreduce API.
         */
        public Text getCurrentValue() {
            return currentValue;
        }

        /**
         * Added to use mapreduce API.
         */
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentValue = new Text();

            return next(currentValue);
        }

        /**
         * Close this RecordReader to future operations.
         */
        public void close() throws IOException {
            inputStream.close();
        }

        /**
         * Create an object of the appropriate type to be used as a key.
         */
        public Text createKey() {
            return new Text();
        }

        /**
         * Create an object of the appropriate type to be used as a value.
         */
        public Text createValue() {
            return new Text();
        }

        /**
         * Returns the current position in the input.
         */
        public long getPos() { 
            return pos; 
        }

        /**
         * How much of the input has the RecordReader consumed i.e.
         */
        public float getProgress() {
            if (start == end)
                return 1.0f;
            else
                return Math.min(1.0f, (pos - start) / (float)(end - start));
        }

        public String makePositionMessage() {
            return file.toString() + ":" + pos;
        }

        protected boolean lowLevelFastqRead(Text readName, Text value) throws IOException {
            // ID line
            readName.clear();
            long skipped = appendLineInto(readName, true);
            pos += skipped;
            if (skipped == 0)
                return false; // EOF
            if (readName.getBytes()[0] != '@')
                throw new RuntimeException("unexpected fastq record didn't start with '@' at " + makePositionMessage() + ". Line: " + readName + ". \n");

            value.append(readName.getBytes(), 0, readName.getLength());

            // sequence
            appendLineInto(value, false);

            // separator line
            appendLineInto(value, false);

            // quality
            appendLineInto(value, false);

            return true;
        }


        /**
         * Reads the next key/value pair from the input for processing.
         */
        public boolean next(Text value) throws IOException {
            if (pos >= end)
                return false; // past end of slice
            try {
                Text readName1 = new Text();
                Text readName2 = new Text();

                value.clear();

                // first read of the pair
                boolean gotData = lowLevelFastqRead(readName1, value);

                if (!gotData)
                    return false;



                // second read of the pair
                gotData = lowLevelFastqRead(readName2, value);

                if (!gotData)
                    return false;

                return true;
            } catch (EOFException e) {
                throw new RuntimeException("unexpected end of file in fastq record at " + makePositionMessage());
            }
        }


        private int appendLineInto(Text dest, boolean eofOk) throws EOFException, IOException {
            Text buf = new Text();
            int bytesRead = lineReader.readLine(buf, MAX_LINE_LENGTH);

            if (bytesRead < 0 || (bytesRead == 0 && !eofOk))
                throw new EOFException();

            dest.append(buf.getBytes(), 0, buf.getLength());
            dest.append(newline, 0, 1);
            pos += bytesRead;

            return bytesRead;
        }
    }

    public RecordReader<Void, Text> createRecordReader(
            InputSplit genericSplit,
            TaskAttemptContext context) throws IOException, InterruptedException {
        context.setStatus(genericSplit.toString());

        // cast as per example in TextInputFormat
        return new InterleavedFastqRecordReader(context.getConfiguration(), 
                                                (FileSplit)genericSplit); 
    }
}
