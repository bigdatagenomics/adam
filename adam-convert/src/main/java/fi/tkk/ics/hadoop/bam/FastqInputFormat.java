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

package fi.tkk.ics.hadoop.bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.Text;
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

import fi.tkk.ics.hadoop.bam.FormatConstants.BaseQualityEncoding;

public class FastqInputFormat extends FileInputFormat<Text,SequencedFragment>
{
	public static final String CONF_BASE_QUALITY_ENCODING = "hbam.fastq-input.base-quality-encoding";
	public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "sanger";

	public static class FastqRecordReader extends RecordReader<Text,SequencedFragment>
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
		private Text currentKey = new Text();
		private SequencedFragment currentValue = new SequencedFragment();

		/* If true, will scan the identifier for read data as specified in the Casava
		 * users' guide v1.8:
		 * @<instrument>:<run number>:<flowcell ID>:<lane>:<tile>:<x-pos>:<y-pos> <read>:<is filtered>:<control number>:<index sequence>
		 * After the first name that doesn't match lookForIlluminaIdentifier will be
		 * set to false and no further scanning will be done.
		 */
		private boolean lookForIlluminaIdentifier = true;
		private static final Pattern ILLUMINA_PATTERN = Pattern.compile("([^:]+):(\\d+):([^:]*):(\\d+):(\\d+):(-?\\d+):(-?\\d+)\\s+([123]):([YN]):(\\d+):(.+)");

		private Text buffer = new Text();

		private BaseQualityEncoding qualityEncoding;

		// How long can a read get?
		private static final int MAX_LINE_LENGTH = 10000;

		public FastqRecordReader(Configuration conf, FileSplit split) throws IOException
		{
			setConf(conf);
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

		protected void setConf(Configuration conf)
		{
			String encoding = conf.get(FastqInputFormat.CONF_BASE_QUALITY_ENCODING, FastqInputFormat.CONF_BASE_QUALITY_ENCODING_DEFAULT);

			if ("illumina".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Illumina;
			else if ("sanger".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Sanger;
			else
				throw new RuntimeException("Unknown " + FastqInputFormat.CONF_BASE_QUALITY_ENCODING + " value " + encoding);
		}

		/*
		 * Position the input stream at the start of the first record.
		 */
		private void positionAtFirstRecord(FSDataInputStream stream) throws IOException
		{
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
		public Text getCurrentKey()
		{
			return currentKey;
		}

		/**
		 * Added to use mapreduce API.
		 */
		public SequencedFragment getCurrentValue()
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
		public SequencedFragment createValue()
		{
			return new SequencedFragment();
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

		/**
		 * Reads the next key/value pair from the input for processing.
		 */
		public boolean next(Text key, SequencedFragment value) throws IOException
		{
			if (pos >= end)
				return false; // past end of slice

			// ID line
			long skipped = lineReader.skip(1); // skip @
			pos += skipped;
			if (skipped == 0)
				return false; // EOF
			try
			{
				// ID
				readLineInto(key);
				// sequence
				value.clear();
				readLineInto(value.getSequence());
				readLineInto(buffer);
				if (buffer.getLength() == 0 || buffer.getBytes()[0] != '+')
					throw new RuntimeException("unexpected fastq line separating sequence and quality at " + makePositionMessage() + ". Line: " + buffer + ". \nSequence ID: " + key);
				readLineInto(value.getQuality());

				if (qualityEncoding == BaseQualityEncoding.Illumina)
				{
					try
					{
						// convert illumina to sanger scale
						SequencedFragment.convertQuality(value.getQuality(), BaseQualityEncoding.Illumina, BaseQualityEncoding.Sanger);
					} catch (FormatException e) {
						throw new FormatException(e.getMessage() + " Position: " + makePositionMessage() + "; Sequence ID: " + key);
					}
				}
				else // sanger qualities.
				{
					int outOfRangeElement = SequencedFragment.verifyQuality(value.getQuality(), BaseQualityEncoding.Sanger);
					if (outOfRangeElement >= 0)
					{
						throw new FormatException("fastq base quality score out of range for Sanger Phred+33 format (found " +
						    (value.getQuality().getBytes()[outOfRangeElement] - FormatConstants.SANGER_OFFSET) + ").\n" +
						    "Although Sanger format has been requested, maybe qualities are in Illumina Phred+64 format?\n" +
						    "Position: " + makePositionMessage() + "; Sequence ID: " + key);
					}
				}

				// look for the Illumina-formatted name.  Once it isn't found lookForIlluminaIdentifier will be set to false
				lookForIlluminaIdentifier = lookForIlluminaIdentifier && scanIlluminaId(key, value);
				if (!lookForIlluminaIdentifier)
					scanNameForReadNumber(key, value);
			}
			catch (EOFException e) {
				throw new RuntimeException("unexpected end of file in fastq record at " + makePositionMessage() + ".  Id: " + key.toString());
			}
			return true;
		}

		private void scanNameForReadNumber(Text name, SequencedFragment fragment)
		{
			// look for a /[0-9] at the end of the name
			if (name.getLength() >= 2)
			{
				byte[] bytes = name.getBytes();
				int last = name.getLength() - 1;

				if (bytes[last-1] == '/' && bytes[last] >= '0' && bytes[last] <= '9')
					fragment.setRead(bytes[last] - '0');
			}
		}

		private boolean scanIlluminaId(Text name, SequencedFragment fragment)
		{
			Matcher m = ILLUMINA_PATTERN.matcher(name.toString());
			boolean matches = m.matches();
			if (matches)
			{
				fragment.setInstrument(m.group(1));
				fragment.setRunNumber(Integer.parseInt(m.group(2)));
				fragment.setFlowcellId(m.group(3));
				fragment.setLane(Integer.parseInt(m.group(4)));
				fragment.setTile(Integer.parseInt(m.group(5)));
				fragment.setXpos(Integer.parseInt(m.group(6)));
				fragment.setYpos(Integer.parseInt(m.group(7)));
				fragment.setRead(Integer.parseInt(m.group(8)));
				fragment.setFilterPassed("N".equals(m.group(9)));
				fragment.setControlNumber(Integer.parseInt(m.group(10)));
				fragment.setIndexSequence(m.group(11));
			}
			return matches;
		}

		private int readLineInto(Text dest) throws EOFException, IOException
		{
			int bytesRead = lineReader.readLine(dest, MAX_LINE_LENGTH);
			if (bytesRead <= 0)
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

	public RecordReader<Text, SequencedFragment> createRecordReader(
	                                        InputSplit genericSplit,
	                                        TaskAttemptContext context) throws IOException, InterruptedException
	{
		context.setStatus(genericSplit.toString());
		return new FastqRecordReader(context.getConfiguration(), (FileSplit)genericSplit); // cast as per example in TextInputFormat
	}
}
