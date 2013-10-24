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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import fi.tkk.ics.hadoop.bam.FormatConstants.BaseQualityEncoding;
import fi.tkk.ics.hadoop.bam.util.ConfHelper;
import parquet.hadoop.util.ContextUtil;

/**
 * Reads the Illumina qseq sequence format.
 * Key: instrument, run number, lane, tile, xpos, ypos, read number, delimited by ':' characters.
 * Value:  a SequencedFragment object representing the entry.
 */
public class QseqInputFormat extends FileInputFormat<Text,SequencedFragment>
{
	public static final String CONF_BASE_QUALITY_ENCODING = "hbam.qseq-input.base-quality-encoding";
	public static final String CONF_FILTER_FAILED_QC      = "hbam.qseq-input.filter-failed-qc";
	public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "illumina";

	public static class QseqRecordReader extends RecordReader<Text,SequencedFragment>
	{
		/*
		 * qseq format:
		 * 11 tab-separated columns
		 *
		 * 1) Instrument
		 * 2) Run id
		 * 3) Lane number
		 * 4) Tile number
		 * 5) X pos
		 * 6) Y pos
		 * 7) Index sequence (0 for runs without multiplexing)
		 * 8) Read Number
		 * 9) Base Sequence
		 * 10) Base Quality
		 * 11) Filter: did the read pass filtering? 0 - No, 1 - Yes.
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

		private Text buffer = new Text();
		private static final int NUM_QSEQ_COLS = 11;
		// for these, we have one per qseq field
		private int[] fieldPositions = new int[NUM_QSEQ_COLS];
		private int[] fieldLengths = new int[NUM_QSEQ_COLS];

		private BaseQualityEncoding qualityEncoding;
		private boolean filterFailedQC = false;

		private static final String Delim = "\t";

		// How long can a qseq line get?
		public static final int MAX_LINE_LENGTH = 20000;

		public QseqRecordReader(Configuration conf, FileSplit split) throws IOException
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

		/*
		 * Position the input stream at the start of the first record.
		 */
		private void positionAtFirstRecord(FSDataInputStream stream) throws IOException
		{
			if (start > 0)
			{
				// Advance to the start of the first line in our slice.
				// We use a temporary LineReader to read a partial line and find the
				// start of the first one on or after our starting position.
				// In case our slice starts right at the beginning of a line, we need to back
				// up by one position and then discard the first line.
				start -= 1;
				stream.seek(start);
				LineReader reader = new LineReader(stream);
				int bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
				start = start + bytesRead;
				stream.seek(start);
			}
			// else
			//	if start == 0 we're starting at the beginning of a line
			pos = start;
		}

		protected void setConf(Configuration conf)
		{
			String encoding =
			  conf.get(QseqInputFormat.CONF_BASE_QUALITY_ENCODING,
			    conf.get(FormatConstants.CONF_INPUT_BASE_QUALITY_ENCODING,
			      CONF_BASE_QUALITY_ENCODING_DEFAULT));

			if ("illumina".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Illumina;
			else if ("sanger".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Sanger;
			else
				throw new RuntimeException("Unknown input base quality encoding value " + encoding);

			filterFailedQC = ConfHelper.parseBoolean(
			  conf.get(QseqInputFormat.CONF_FILTER_FAILED_QC,
			    conf.get(FormatConstants.CONF_INPUT_FILTER_FAILED_QC)),
			      false);
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

		public String makePositionMessage(long pos)
		{
			return file.toString() + ":" + pos;
		}

		public String makePositionMessage()
		{
			return file.toString() + ":" + pos;
		}

		/*
		 * Read a single record.
		 *
		 * Reads a single line of input and scans it with scanQseqLine, which
		 * sets key and value accordingly.  The method updates this.pos.
		 *
		 * @return The number of bytes read.  If no bytes were read, the EOF was reached.
		 */
		private int lowLevelQseqRead(Text key, SequencedFragment value) throws IOException
		{
			int bytesRead = lineReader.readLine(buffer, MAX_LINE_LENGTH);
			pos += bytesRead;
			if (bytesRead >= MAX_LINE_LENGTH)
			{
				String line;
				try {
					line = Text.decode(buffer.getBytes(), 0, 500);
				} catch (java.nio.charset.CharacterCodingException e) {
					line = "(line not convertible to printable format)";
				}
				throw new RuntimeException("found abnormally large line (length " + bytesRead + ") at " +
				            makePositionMessage(pos - bytesRead) + ": " + line);
			}
			else if (bytesRead > 0)
				scanQseqLine(buffer, key, value);

			return bytesRead;
		}

		/**
		 * Reads the next key/value pair from the input for processing.
		 */
		public boolean next(Text key, SequencedFragment value) throws IOException
		{
			if (pos >= end)
				return false; // past end of slice

			int bytesRead = 0;
			boolean goodRecord;
			do {
				bytesRead = lowLevelQseqRead(key, value); // if bytesRead <= 0 EOF has been reached
				goodRecord = (bytesRead > 0) && (!filterFailedQC || value.getFilterPassed() == null || value.getFilterPassed());
			} while (bytesRead > 0 && !goodRecord);

			if (goodRecord) // post process the record only if it's going to be used
			{
				try {
					postProcessSequencedFragment(value);
				} catch (FormatException e) {
					throw new FormatException(e.getMessage() + " Position: " + makePositionMessage(this.pos - bytesRead) +
					            "; line: " + buffer); // last line read is still in the buffer
				}
			}

			return goodRecord;
		}

		/*
		 * Scans the text line to find the position and the lengths of the fields
		 * within it. The positions and lengths are saved into the instance arrays
		 * 'fieldPositions' and 'fieldLengths'.
		 *
		 * @exception FormatException Line doesn't have the expected number of fields.
		 */
		private void setFieldPositionsAndLengths(Text line)
		{
			int pos = 0; // the byte position within the record
			int fieldno = 0; // the field index within the record
			while (pos < line.getLength() && fieldno < NUM_QSEQ_COLS) // iterate over each field
			{
				int endpos = line.find(Delim, pos); // the field's end position
				if (endpos < 0)
					endpos = line.getLength();

				fieldPositions[fieldno] = pos;
				fieldLengths[fieldno] = endpos - pos;

				pos = endpos + 1; // the next starting position is the current end + 1
				fieldno += 1;
			}

			if (fieldno != NUM_QSEQ_COLS)
				throw new FormatException("found " + fieldno + " fields instead of 11 at " +
				            makePositionMessage(this.pos - line.getLength()) + ". Line: " + line);
		}

		private void scanQseqLine(Text line, Text key, SequencedFragment fragment)
		{
			setFieldPositionsAndLengths(line);

			// Build the key.  We concatenate all fields from 0 to 5 (machine to y-pos)
			// and then the read number, replacing the tabs with colons.
			key.clear();
			// append up and including field[5]
			key.append(line.getBytes(), 0, fieldPositions[5] + fieldLengths[5]);
			// replace tabs with :
			byte[] bytes = key.getBytes();
			int temporaryEnd = key.getLength();
			for (int i = 0; i < temporaryEnd; ++i)
				if (bytes[i] == '\t')
					bytes[i] = ':';
			// append the read number
			key.append(line.getBytes(), fieldPositions[7] - 1, fieldLengths[7] + 1); // +/- 1 to catch the preceding tab.
			// convert the tab preceding the read number into a :
			key.getBytes()[temporaryEnd] = ':';

			// now the fragment
			try
			{
				fragment.clear();
				fragment.setInstrument( Text.decode(line.getBytes(), fieldPositions[0], fieldLengths[0]) );
				fragment.setRunNumber( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[1], fieldLengths[1])) );
				//fragment.setFlowcellId();
				fragment.setLane( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[2], fieldLengths[2])) );
				fragment.setTile( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[3], fieldLengths[3])) );
				fragment.setXpos( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[4], fieldLengths[4])) );
				fragment.setYpos( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[5], fieldLengths[5])) );
				fragment.setRead( Integer.parseInt(Text.decode(line.getBytes(), fieldPositions[7], fieldLengths[7])) );
				fragment.setFilterPassed( line.getBytes()[fieldPositions[10]] != '0' );
				//fragment.setControlNumber();
				if (fieldLengths[6] > 0 && line.getBytes()[fieldPositions[6]] == '0') // 0 is a null index sequence
					fragment.setIndexSequence(null);
				else
					fragment.setIndexSequence(Text.decode(line.getBytes(), fieldPositions[6], fieldLengths[6]).replace('.', 'N'));
			}
			catch (CharacterCodingException e) {
				throw new FormatException("Invalid character format at " + makePositionMessage(this.pos - line.getLength()) + "; line: " + line);
			}

			fragment.getSequence().append(line.getBytes(), fieldPositions[8], fieldLengths[8]);
			fragment.getQuality().append(line.getBytes(), fieldPositions[9], fieldLengths[9]);
		}

		/*
		 * This method applies some transformations to the read and quality data.
		 *
		 * <ul>
		 *   <li>'.' in the read are converted to 'N'</li>
		 *   <li>the base quality encoding is converted to 'sanger', unless otherwise
		 *   requested by the configuration.</li>
		 * </ul>
		 *
		 * @exception FormatException Thrown if the record contains base quality scores
		 * outside the range allowed by the format.
		 */
		private void postProcessSequencedFragment(SequencedFragment fragment)
		{
			byte[] bytes = fragment.getSequence().getBytes();
			// replace . with N
			for (int i = 0; i < fieldLengths[8]; ++i)
				if (bytes[i] == '.')
					bytes[i] = 'N';

			if (qualityEncoding == BaseQualityEncoding.Illumina)
			{
				// convert illumina to sanger scale
				SequencedFragment.convertQuality(fragment.getQuality(), BaseQualityEncoding.Illumina, BaseQualityEncoding.Sanger);
			}
			else // sanger qualities.
			{
				int outOfRangeElement = SequencedFragment.verifyQuality(fragment.getQuality(), BaseQualityEncoding.Sanger);
				if (outOfRangeElement >= 0)
				{
					throw new FormatException("qseq base quality score out of range for Sanger Phred+33 format (found " +
					    (fragment.getQuality().getBytes()[outOfRangeElement] - FormatConstants.SANGER_OFFSET) + ").\n" +
					    "Although Sanger format has been requested, maybe qualities are in Illumina Phred+64 format?\n");
				}
			}
		}
	}

	@Override
	public boolean isSplitable(JobContext context, Path path)
	{
		CompressionCodec codec = new CompressionCodecFactory(ContextUtil.getConfiguration(context)).getCodec(path);
		return codec == null;
	}

	public RecordReader<Text, SequencedFragment> createRecordReader(
	                                        InputSplit genericSplit,
	                                        TaskAttemptContext context) throws IOException, InterruptedException
	{
		context.setStatus(genericSplit.toString());
		return new QseqRecordReader(ContextUtil.getConfiguration(context), (FileSplit)genericSplit); // cast as per example in TextInputFormat
	}
}
