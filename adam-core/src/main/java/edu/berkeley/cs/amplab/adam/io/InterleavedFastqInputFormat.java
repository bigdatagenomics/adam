// Copyright (C) 2011-2012 CRS4.
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

package edu.berkeley.cs.amplab.adam.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;

import java.util.regex.*;

public class InterleavedFastqInputFormat extends FileInputFormat<Void,Text>
{

	public static class InterleavedFastqRecordReader extends RecordReader<Void,Text>
	{
		/*
		 * fastq format:
		 * <fastq>	:=	<block>+
		 * <block>	:=	@<seqname>\n<seq>\n+[<seqname>]\n<qual>\n
		 * <seqname>	:=	[A-Za-z0-9_.:-]+
		 * <seq>	:=	[A-Za-z\n\.~]+
		 * <qual>	:=	[!-~\n]+
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
		private Text currentValue = new Text();

		/* If true, will scan the identifier for read data as specified in the Casava
		 * users' guide v1.8:
		 * @<instrument>:<run number>:<flowcell ID>:<lane>:<tile>:<x-pos>:<y-pos> <read>:<is filtered>:<control number>:<index sequence>
		 * After the first name that doesn't match lookForIlluminaIdentifier will be
		 * set to false and no further scanning will be done.
		 */
		private boolean lookForIlluminaIdentifier = true;
		private static final Pattern ILLUMINA_PATTERN = Pattern.compile("([^:]+):(\\d+):([^:]*):(\\d+):(\\d+):(-?\\d+):(-?\\d+)\\s+([123]):([YN]):(\\d+):(.*)");

		// How long can a read get?
		private static final int MAX_LINE_LENGTH = 10000;

		public InterleavedFastqRecordReader(Configuration conf, FileSplit split) throws IOException
		{
			file = split.getPath();
			start = split.getStart();
			end = start + split.getLength();

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
			Text buffer = new Text();

			if (start > 0)
			{
				// Advance to the start of the first record
				// We use a temporary LineReader to read lines until we find the
				// position of the right one.  We then seek the file to that position.
				stream.seek(start);
				LineReader reader = new LineReader(stream);

				int bytesRead = 0;
				do
			 	{
					bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
					if (bytesRead > 0 && (buffer.getLength() <= 0 || buffer.getBytes()[0] != '@'))
						start += bytesRead;
					else
					{
						// line starts with @.  Read two more and verify that it starts with a +
						//
						// If this isn't the start of a record, we want to backtrack to its end
						long backtrackPosition = start + bytesRead;

						bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
						bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
						if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+')
							break; // all good!
						else
						{
							// backtrack to the end of the record we thought was the start.
							start = backtrackPosition;
							stream.seek(start);
							reader = new LineReader(stream);
						}
					}
				} while (bytesRead > 0);

				stream.seek(start);
			}
			// else
			//	if start == 0 we presume it starts with a valid fastq record
			pos = start;
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
		public Void getCurrentKey()
		{
			return null;
		}

		/**
		 * Added to use mapreduce API.
		 */
		public Text getCurrentValue()
	 	{
			return currentValue;
		}

		/**
		 * Added to use mapreduce API.
		 */
		public boolean nextKeyValue() throws IOException, InterruptedException
		{
			return next(currentValue);
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
		public Void createKey()
		{
			return null;
		}

		/**
		 * Create an object of the appropriate type to be used as a value.
		 */
		public Text createValue()
		{
			return new Text();
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

		public String makePositionMessage()
		{
			return file.toString() + ":" + pos;
		}

		protected boolean lowLevelFastqRead(Text readName, Text value) throws IOException
		{
			// ID line
			readName.clear();
			long skipped = readLineInto(readName, true);
			pos += skipped;
			if (skipped == 0)
				return false; // EOF
			if (readName.getBytes()[0] != '@')
				throw new RuntimeException("unexpected fastq record didn't start with '@' at " + makePositionMessage() + ". Line: " + readName + ". \n");

			value.append(readName.getBytes(), 0, readName.getLength());

			// sequence
			readLineInto(value, false);

			// separator line
			readLineInto(value, false);

			// quality
			readLineInto(value, false);

			return true;
		}


		/**
		 * Reads the next key/value pair from the input for processing.
		 */
		public boolean next(Text value) throws IOException
		{
			if (pos >= end)
				return false; // past end of slice
			try
			{
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

				// TODO: make sure read one ends with /1, the other ends with /2

				return true;
			}
			catch (EOFException e) {
				throw new RuntimeException("unexpected end of file in fastq record at " + makePositionMessage());
			}
		}


		private int readLineInto(Text dest, boolean eofOk) throws EOFException, IOException
		{
			int bytesRead = lineReader.readLine(dest, MAX_LINE_LENGTH);
			if (bytesRead < 0 || (bytesRead == 0 && !eofOk))
				throw new EOFException();
			pos += bytesRead;
			return bytesRead;
		}
	}

	@Override
	public boolean isSplitable(JobContext context, Path path)
	{
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(path);
		return codec == null;
	}

	public RecordReader<Void, Text> createRecordReader(
	                                        InputSplit genericSplit,
	                                        TaskAttemptContext context) throws IOException, InterruptedException
	{
		context.setStatus(genericSplit.toString());
		return new InterleavedFastqRecordReader(context.getConfiguration(), (FileSplit)genericSplit); // cast as per example in TextInputFormat
	}
}
