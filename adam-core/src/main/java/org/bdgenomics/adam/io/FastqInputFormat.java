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
 * This reader is based on the FastqInputFormat that's part of Hadoop-BAM,
 * found at https://github.com/HadoopGenomics/Hadoop-BAM/blob/master/src/main/java/org/seqdoop/hadoop_bam/FastqInputFormat.java
 *
 * @author Jeremy Elson (jelson@microsoft.com)
 * @author Frank Austin Nothaft (fnothaft@berkeley.edu)
 * @date Feb 2015
 */
abstract public class FastqInputFormat extends FileInputFormat<Void,Text> {
    
    abstract public static class FastqRecordReader extends RecordReader<Void,Text> {
        /*
         * fastq format:
         * <fastq>  :=  <block>+
         * <block>  :=  @<seqname>\n<seq>\n\+[<seqname>]\n<qual>\n
         * <seqname>  :=  [A-Za-z0-9_.:-]+
         * <seq>  :=  [A-Za-z\n\.~]+
         * <qual> :=  [!-~\n]+
         */
        
        // start:  first valid data index
        protected long start;
        // end:  first index value beyond the slice, i.e. slice is in range [start,end)
        protected long end;
        // pos: current position in file
        protected long pos;
        // file:  the file being read
        protected Path file;

        protected FSDataInputStream inputStream;
        protected Text currentValue;
        protected byte[] newline = "\n".getBytes();

        public FastqRecordReader(Configuration conf, FileSplit split) throws IOException {
            file = split.getPath();
            start = split.getStart();
            end = start + split.getLength();

            FileSystem fs = file.getFileSystem(conf);
            inputStream = fs.open(file);
            
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
            CompressionCodec        codec        = codecFactory.getCodec(file);

            if (codec == null) { // no codec.  Uncompressed file.
                positionAtFirstRecord(inputStream);
            } else { 
                throw new IllegalArgumentException("We currently do not support compressed FASTQ.");
            }
        }

        /**
         * Position the input stream at the start of the first record.
         */
        abstract protected void positionAtFirstRecord(FSDataInputStream stream) throws IOException;

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

        protected int getLineInfo(LineReader reader) throws IOException {
            Text buffer = new Text();
            int sequenceLines=1;
            int bytesRead = 0;
            
            //starts at @title line. Read 2 lines and see if you have reached +secondTitle
            bytesRead = reader.readLine(buffer);
	    if (buffer.getBytes()[0] != '@') {
		throw new IllegalStateException("Start was placed at " + buffer + ", not @.");
	    }
            bytesRead = reader.readLine(buffer);
	    bytesRead = reader.readLine(buffer);
            
            if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+') {
                //read is 1 line. Read 2 more lines to position the reader at the next @title line
                bytesRead = reader.readLine(buffer);
                return sequenceLines; // all good!
            } else {
                String bufferString = buffer.toString();
                
                //regex string that matches only IUPAC codes
                String iupacRegex = "[A-Y-[J,O,X]|.|-]+";

                // check if the record is multiline and we are still reading the sequence data
                while (bytesRead > 0 && buffer.getLength() > 0 && bufferString.matches(iupacRegex)) {
                    // read another line, and count the number that are read.
                    bytesRead = reader.readLine(buffer);
                    sequenceLines++; 

                    // check if we have reached the second title line
                    if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+') { 
                        // number of lines has been determined. Set reader to next read's @title line.
                        for (int x = 0; x<=sequenceLines; x++) {
                            bytesRead = reader.readLine(buffer);
                        }
                        return sequenceLines; // all good!
                    }
                    bufferString = buffer.toString();
                }
            }
            throw new IllegalStateException("Record does not seem to be FASTQ formatted.");
        }

        protected boolean lowLevelFastqRead(Text readName, Text value) throws IOException {
            // construct line reader
            inputStream.seek(pos);
            LineReader lineReader = new LineReader(inputStream);
            
            // ID line
            readName.clear();
            long skipped = appendLineInto(readName, true);
            if (skipped == 0)
                return false; // EOF
            if (readName.getBytes()[0] != '@')
                throw new RuntimeException("unexpected fastq record didn't start with '@' at " + makePositionMessage() + ". Line: " + readName + ". \n");
            pos += skipped;

            value.append(readName.getBytes(), 0, readName.getLength());
            Text tempReadBuffer = new Text();
            int bytesRead = 0;
            
            // sequence
            do {
                if (tempReadBuffer.getLength() > 0) {
                    value.append(tempReadBuffer.getBytes(), 0, tempReadBuffer.getLength());
                    value.append(newline, 0, 1);
                } else {
                    tempReadBuffer.clear();
                }
                inputStream.seek(pos);
                lineReader = new LineReader(inputStream);
                bytesRead = lineReader.readLine(tempReadBuffer);
                pos += bytesRead;
            } while (bytesRead > 0 && tempReadBuffer.getLength() > 0 && tempReadBuffer.getBytes()[0] != '+');
            
            // separator line
            if (bytesRead < 0) {
                throw new EOFException();
            }

            // qualities
            do {
                if (tempReadBuffer.getLength() > 0) {
                    value.append(tempReadBuffer.getBytes(), 0, tempReadBuffer.getLength());
                    value.append(newline, 0, 1);
                } else {
                    tempReadBuffer.clear();
                }
                inputStream.seek(pos);
                lineReader = new LineReader(inputStream);
                bytesRead = lineReader.readLine(tempReadBuffer);
                pos += bytesRead;
            } while (bytesRead > 0 && tempReadBuffer.getLength() > 0 && tempReadBuffer.getBytes()[0] != '@');

            // we must backtrack to the start of the last line, which is the start of the next record
            pos -= bytesRead;

            return true;
        }

        protected int appendLineInto(Text dest, boolean eofOk) throws EOFException, IOException {
            Text buf = new Text();
            LineReader lineReader = new LineReader(inputStream);
            int bytesRead = lineReader.readLine(buf);

            if (bytesRead < 0 || (bytesRead == 0 && !eofOk)) {
                throw new EOFException();
            }

            dest.append(buf.getBytes(), 0, buf.getLength());
            dest.append(newline, 0, 1);

            return bytesRead;
        }

        /**
         * Reads the next key/value pair from the input for processing.
         */
        abstract public boolean next(Text value) throws IOException;
    }

    abstract public RecordReader<Void, Text> createRecordReader(
            InputSplit genericSplit,
            TaskAttemptContext context) throws IOException, InterruptedException;
}
