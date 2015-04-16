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
 * @author Jeremy Elson (jelson@microsoft.com)
 * @author Frank Austin Nothaft (fnothaft@berkeley.edu)
 * @date Feb 2015
 */
public class InterleavedFastqInputFormat extends FastqInputFormat {
    
    public static class InterleavedFastqRecordReader extends FastqRecordReader {

        public InterleavedFastqRecordReader(Configuration conf, FileSplit split) throws IOException {
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
				      buffer.getBytes()[0] != '@' ||
				      (bufferLength >= 2 && buffer.getBytes()[bufferLength - 2] != '/') ||
				      (bufferLength >= 1 && buffer.getBytes()[bufferLength - 1] != '1'))) {
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
