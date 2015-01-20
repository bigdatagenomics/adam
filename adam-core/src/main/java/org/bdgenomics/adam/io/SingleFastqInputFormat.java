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
 * This class is a Hadoop reader for single read fastq.
 *
 * This reader is based on the FastqInputFormat that's part of Hadoop-BAM,
 * found at https://github.com/HadoopGenomics/Hadoop-BAM/blob/master/src/main/java/org/seqdoop/hadoop_bam/FastqInputFormat.java
 *
 * @author Frank Austin Nothaft (fnothaft@berkeley.edu)
 * @date September 2014
 */
public class SingleFastqInputFormat extends FileInputFormat<Void,Text> {
    
    public static class SingleFastqRecordReader extends RecordReader<Void,Text> {
        /*
         * fastq format:
         * <fastq>  :=  <block>+
         * <block>  :=  @<seqname>\n<seq>\n\+[<seqname>]\n<qual>\n
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
         *
         * Multiline sequences are now supported. A separate line reader determines the number 
         * of lines in the read before lineReader parses them.
         */
        
        // start:  first valid data index
        private long start;
        // end:  first index value beyond the slice, i.e. slice is in range [start,end)
        private long end;
        // pos: current position in file
        private long pos;
        // file:  the file being read
        private Path file;
        //number of lines in the first read
        private int readLines;

        /*reader is used to determine the position of the first record and to determine the number of lines in 
        *each record before they are parsed by lineReader
        */
        private LineReader reader; 
        private LineReader lineReader;
       
        private InputStream inputStream;
        private Text currentValue;
        private byte[] newline = "\n".getBytes();

        // How long can a read get?
        private static final int MAX_LINE_LENGTH = 10000;

        public SingleFastqRecordReader(Configuration conf, FileSplit split) throws IOException {
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
            int bytesRead = 0;

                // Advance to the start of the first record
                // We use reader to read lines until we find the
                // position of the right one.  We then seek the file to that position.
                stream.seek(start);
                reader = new LineReader(stream);

                do {
                    bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start)); 
                    int bufferLength = buffer.getLength();
                    if (bytesRead > 0 && (bufferLength <= 0 ||
                                          buffer.getBytes()[0] != '@')) {
                        start += bytesRead;
                    } else {
                        // line starts with @.  Read two more and verify that it starts with a +
                        //
                        // If this isn't the start of a record, we want to backtrack to its end
                        long backtrackPosition = start + bytesRead; 

                        //determines if read is fastq and if it is single/multi lined. Returns number of lines, and -1 if not fastq format.
                        int lines = getLineInfo();
                        
                        if(lines==-1){
                            // backtrack to the end of the record we thought was the start.
                            start = backtrackPosition;
                            stream.seek(start);
                            reader = new LineReader(stream);
                            
                        }else{
                                readLines = lines; //to avoid calling getLineInfo on the first read again, the line info is saved
                                break; //all good!
                            }
                        
                    }

                } while (bytesRead > 0);

                stream.seek(start);

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
        private int getLineInfo() throws IOException {
            Text buffer = new Text();
            int sequenceLines=1;
            int bytesRead = 0;

            //starts at @title line. Read 2 lines and see if you have reached +secondTitle
            bytesRead = reader.readLine(buffer,(int)Math.min(MAX_LINE_LENGTH, end - start));
            bytesRead = reader.readLine(buffer,(int)Math.min(MAX_LINE_LENGTH, end - start));

            if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+') {

                //read is 1 line. Read 2 more lines to position the reader at the next @title line
                bytesRead = reader.readLine(buffer,(int)Math.min(MAX_LINE_LENGTH, end - start));
                bytesRead = reader.readLine(buffer,(int)Math.min(MAX_LINE_LENGTH, end - start));
                return sequenceLines; // all good!

            } else {
                String bufferString = buffer.toString();
                
                //regex string that matches only IUPAC codes
                String iupacRegex = "[A-Y-[J,O,X]|.|-]+";

                //check if the record is multiline and we are still reading the sequence data
                while(bytesRead > 0 && buffer.getLength() > 0 && bufferString.matches(iupacRegex)){ 

                    //read another line, and count the number that are read.
                    bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
                    sequenceLines++; 

                    //check if we have reached the second title line
                    if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+') { 

                        //number of lines has been determined. Set reader to next read's @title line.
                        for(int x = 0; x<=sequenceLines; x++){
                            bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start)); 
                        }
                        return sequenceLines; // all good!
                    }
                    bufferString = buffer.toString();
                }
            }
            return -1;
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
            int sequenceLines;
            //check if getLineInfo has already been called on this read
            if(readLines==-2){
                //it has not been called
                sequenceLines = getLineInfo();
            }else{
                //it has been called. Take line info from readLines and set it to -2.
                sequenceLines=readLines;
                readLines=-2;
            }

            // sequence
            for(int x = 0; x<sequenceLines; x++) {
            appendLineInto(value, false);
            }

            // separator line
            appendLineInto(value, false);

            // quality
            for(int x = 0; x<sequenceLines; x++) {
            appendLineInto(value, false);
            }
            

            return true;
        }


        /**
         * Reads the next key/value pair from the input for processing.
         */
        public boolean next(Text value) throws IOException {
            if (pos >= end)
                return false; // past end of slice
            try {
                Text readName = new Text();

                value.clear();

                // first read of the pair
                boolean gotData = lowLevelFastqRead(readName, value);

                return gotData;
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
        return new SingleFastqRecordReader(context.getConfiguration(), 
                                           (FileSplit)genericSplit); 
    }
}
