// Copyright (c) 2012 Aalto University
//
// This file is part of Hadoop-BAM.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package fi.tkk.ics.hadoop.bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import parquet.hadoop.util.ContextUtil;

import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Reads the FASTA reference sequence format.
 * Key: sequence description and position offset, delimited by ':' characters.
 * Value:  a ReferenceFragment object representing the entry.
 *
 * Note: here sections in the input file are assumed to be delimited by single
 * line descriptions that start with '>'.
 */
public class FastaInputFormat extends FileInputFormat<Text,ReferenceFragment>
{

    @Override public List<InputSplit> getSplits(JobContext job) throws IOException
	{

	    // Note: We generate splits that correspond to different sections in the FASTA
	    // input (which here are called "chromosomes", delimited by '>' and
	    // followed by a single line description.
	    // Some locality is preserved since the locations are formed from the input
	    // splits, although no special attention is given to this issues (FASTA files
	    // are assumed to be smallish).
	    // The splits are generated on the client. In the future the split generation
	    // should be only performed once and an index file stored inside HDFS for
	    // peformance reasons. Currently this is not attempted (again: FASTA files
	    // aren't all that big).

	    // we first make sure we are given only a single file

            List<InputSplit> splits = super.getSplits(job);
            
            // first sort by input path
            Collections.sort(splits, new Comparator<InputSplit>()
                             {
                                 public int compare(InputSplit a, InputSplit b) {
                                     FileSplit fa = (FileSplit)a, fb = (FileSplit)b;
                                     return fa.getPath().compareTo(fb.getPath());
                                 }
                             });

            for (int i = 0; i < splits.size()-1;) {
                FileSplit fa = (FileSplit)splits.get(i);
                FileSplit fb = (FileSplit)splits.get(i+1);
                    
                if(fa.getPath().compareTo(fb.getPath()) != 0)
                    throw new IOException("FastaInputFormat assumes single FASTA input file!");
            }

            // now we are sure we only have one FASTA input file

	    final List<InputSplit> newSplits = new ArrayList<InputSplit>(splits.size());
	    FileSplit fileSplit = (FileSplit)splits.get(0);
	    Path path = fileSplit.getPath();

	    FileSystem fs = path.getFileSystem(ContextUtil.getConfiguration(job));
	    FSDataInputStream fis = fs.open(path);
	    byte[] buffer = new byte[1024];

	    long byte_counter = 0;
	    long prev_chromosome_byte_offset = 0;
	    boolean first_chromosome = true;

	    for(int j = 0; j < splits.size(); j++) {
		FileSplit origsplit = (FileSplit)splits.get(j);

		while(byte_counter < origsplit.getStart()+origsplit.getLength()) {
		    long bytes_read = fis.read(byte_counter, buffer, 0, (int)Math.min(buffer.length,
										      origsplit.getStart()+origsplit.getLength()- byte_counter));
		    //System.err.println("bytes_read: "+Integer.toString((int)bytes_read)+" of "+Integer.toString(splits.size())+" splits");
		    if(bytes_read > 0) {
			for(int i=0;i<bytes_read;i++) {
			    if(buffer[i] == (byte)'>') {
				//System.err.println("found chromosome at position "+Integer.toString((int)byte_counter+i));
				
				if(!first_chromosome) {
				    FileSplit fsplit = new FileSplit(path, prev_chromosome_byte_offset, byte_counter + i-1 - prev_chromosome_byte_offset, origsplit.getLocations());
				    //System.err.println("adding split: start: "+Integer.toString((int)fsplit.getStart())+" length: "+Integer.toString((int)fsplit.getLength()));
				    
				    newSplits.add(fsplit);
				}
				first_chromosome = false;
				prev_chromosome_byte_offset = byte_counter + i;
			    }
			}
			byte_counter += bytes_read;
		    }
		}

		if(j == splits.size()-1) {
		    //System.err.println("EOF");
		    FileSplit fsplit = new FileSplit(path, prev_chromosome_byte_offset, byte_counter - prev_chromosome_byte_offset, origsplit.getLocations());
		    newSplits.add(fsplit); //conf));
		    //System.err.println("adding split: "+fsplit.toString());
		    break;
		}
	    }
	    
	    return newSplits;
	}

	public static class FastaRecordReader extends RecordReader<Text,ReferenceFragment>
	{
		
		// start:  first valid data index
		private long start;
		// end:  first index value beyond the slice, i.e. slice is in range [start,end)
		private long end;
		// pos: current position in file
		private long pos;
		// file:  the file being read
		private Path file;

		// current_split_pos: the current (chromosome) position within the split
		private int current_split_pos;
		// current_split_indexseq: the description/chromosome name
		private String current_split_indexseq = null;

		private LineReader lineReader;
		private InputStream inputStream;
		private Text currentKey = new Text();
		private ReferenceFragment currentValue = new ReferenceFragment();

		private Text buffer = new Text();

		// How long can a FASTA line get?
		public static final int MAX_LINE_LENGTH = 20000;

		public FastaRecordReader(Configuration conf, FileSplit split) throws IOException
		{
			setConf(conf);
			file = split.getPath();
			start = split.getStart();
			end = start + split.getLength();
			current_split_pos = 1;

			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream fileIn = fs.open(file);

			CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
			CompressionCodec        codec        = codecFactory.getCodec(file);

			if (codec == null) // no codec.  Uncompressed file.
			{
				positionAtFirstRecord(fileIn);
				inputStream = fileIn;
			}
			else
			{ // compressed file
				if (start != 0)
					throw new RuntimeException("Start position for compressed file is not 0! (found " + start + ")");

				inputStream = codec.createInputStream(fileIn);
				end = Long.MAX_VALUE; // read until the end of the file
			}

			lineReader = new LineReader(inputStream);
		}

		/*
		 * Position the input stream at the start of the first record.
		 */
		private void positionAtFirstRecord(FSDataInputStream stream) throws IOException
		{
		    if (start > 0)
			{
			    stream.seek(start);
			}

		    // we are now in a new chromosome/fragment, so read its name/index sequence
		    // and reset position counter

		    // index sequence
		    LineReader reader = new LineReader(stream);
		    int bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));

		    current_split_indexseq = buffer.toString();
		    // now get rid of '>' character
		    current_split_indexseq = current_split_indexseq.substring(1,current_split_indexseq.length());
		    
		    // initialize position counter
		    current_split_pos = 1;
		    
		    //System.err.println("read index sequence: "+current_split_indexseq);
		    start = start + bytesRead;
		    stream.seek(start);
		    pos = start;
		}

		protected void setConf(Configuration conf)
		{
		}

		/**
		 * Added to use mapreduce API.
		 */
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
		{
		}

		/**
		 * Added to use mapreduce API.
		 */
		public Text getCurrentKey()
		{
			return currentKey;
		}

		/**
		 * Added to use mapreduce API.
		 */
		public ReferenceFragment getCurrentValue()
	 	{
			return currentValue;
		}

		/**
		 * Added to use mapreduce API.
		 */
		public boolean nextKeyValue() throws IOException, InterruptedException
		{
			return next(currentKey, currentValue);
		}

		/**
		 * Close this RecordReader to future operations.
		 */
		public void close() throws IOException
		{
			inputStream.close();
		}

		/**
		 * Create an object of the appropriate type to be used as a key.
		 */
		public Text createKey()
		{
			return new Text();
		}

		/**
		 * Create an object of the appropriate type to be used as a value.
		 */
		public ReferenceFragment createValue()
		{
			return new ReferenceFragment();
		}

		/**
		 * Returns the current position in the input.
		 */
		public long getPos() { return pos; }

		/**
		 * How much of the input has the RecordReader consumed i.e.
		 */
		public float getProgress()
		{
			if (start == end)
				return 1.0f;
			else
				return Math.min(1.0f, (pos - start) / (float)(end - start));
		}

		public String makePositionMessage(long pos)
		{
			return file.toString() + ":" + pos;
		}

		public String makePositionMessage()
		{
			return file.toString() + ":" + pos;
		}

		/**
		 * Reads the next key/value pair from the input for processing.
		 */
		public boolean next(Text key, ReferenceFragment value) throws IOException
		{
			if (pos >= end)
				return false; // past end of slice

			int bytesRead = lineReader.readLine(buffer, MAX_LINE_LENGTH);
			pos += bytesRead;
			if (bytesRead >= MAX_LINE_LENGTH)
				throw new RuntimeException("found abnormally large line (length " + bytesRead + ") at " + makePositionMessage(pos - bytesRead) + ": " + Text.decode(buffer.getBytes(), 0, 500));
			else if (bytesRead <= 0)
				return false; // EOF
			else
			{
				scanFastaLine(buffer, key, value);
				current_split_pos += bytesRead;
				return true;
			}
		}

		private void scanFastaLine(Text line, Text key, ReferenceFragment fragment)
		{
		    // Build the key.  We concatenate the chromosome/fragment descripion and
		    // the start position of the FASTA sequence line, replacing the tabs with colons.
		    key.clear();
		    
		    key.append(current_split_indexseq.getBytes(), 0, current_split_indexseq.getBytes().length);
		    key.append(Integer.toString(current_split_pos).getBytes(), 0, Integer.toString(current_split_pos).getBytes().length);
		    // replace tabs with :
		    byte[] bytes = key.getBytes();
		    int temporaryEnd = key.getLength();
		    for (int i = 0; i < temporaryEnd; ++i)
			if (bytes[i] == '\t')
			    bytes[i] = ':';
		    
		    fragment.clear();
		    fragment.setPosition(current_split_pos);
		    fragment.setIndexSequence(current_split_indexseq);
		    fragment.getSequence().append(line.getBytes(), 0, line.getBytes().length);
		}
	}

	@Override
	public boolean isSplitable(JobContext context, Path path)
	{
		CompressionCodec codec = new CompressionCodecFactory(ContextUtil.getConfiguration(context)).getCodec(path);
		return codec == null;
	}

	public RecordReader<Text, ReferenceFragment> createRecordReader(
	                                        InputSplit genericSplit,
	                                        TaskAttemptContext context) throws IOException, InterruptedException
	{
		context.setStatus(genericSplit.toString());
		return new FastaRecordReader(ContextUtil.getConfiguration(context), (FileSplit)genericSplit); // cast as per example in TextInputFormat
	}
}
