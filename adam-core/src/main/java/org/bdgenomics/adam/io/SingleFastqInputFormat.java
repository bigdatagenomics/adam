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
 * @author Frank Austin Nothaft (fnothaft@berkeley.edu)
 * @date February 2015
 */
public class SingleFastqInputFormat extends FastqInputFormat {
    
    public static class SingleFastqRecordReader extends FastqRecordReader {

        public SingleFastqRecordReader(Configuration conf, FileSplit split) throws IOException {
            super(conf, split);
        }

        /**
         * Position the input stream at the start of the first record.
         */
        protected void positionAtFirstRecord(FSDataInputStream stream) throws IOException {
            Text buffer = new Text();
            int bytesRead = 0;
            
	    // Advance to the start of the first record that ends with /1
	    // We use a temporary LineReader to read lines until we find the
	    // position of the right one.  We then seek the file to that position.
	    stream.seek(start);
	    LineReader reader = new LineReader(stream);
	    do {
		bytesRead = reader.readLine(buffer);
		int bufferLength = buffer.getLength();
		if (bytesRead > 0 && (bufferLength <= 0 ||
				      buffer.getBytes()[0] != '@')) {
		    start += bytesRead;
		} else {
		    // line starts with @.  Read two more and verify that it starts with a +
		    //
		    // If this isn't the start of a record, we want to backtrack to its end
		    long backtrackPosition = start; // + bytesRead;
		    
		    //determines if read is fastq and if it is single/multi lined. Returns number of lines, and -1 if not fastq format.
		    stream.seek(start);
                    start = backtrackPosition;
                    break; //all good!
		}
	    } while (bytesRead > 0);
            
            pos = start;
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
