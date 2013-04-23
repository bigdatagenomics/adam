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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import fi.tkk.ics.hadoop.bam.FormatConstants.BaseQualityEncoding;

/**
 * Output format for Illumina qseq format.
 * Records are lines of tab-separated fields.  Each record consists of
 *   - Machine name
 *   - Run number
 *   - Lane number
 *   - Tile number
 *   - X coordinate of the spot. Integer (can be negative).
 *   - Y coordinate of the spot. Integer (can be negative).
 *   - Index
 *   - Read Number
 *   - Sequence
 *   - Quality
 *   - Filter
 */

public class QseqOutputFormat extends TextOutputFormat<Text, SequencedFragment>
{
	public static final String CONF_BASE_QUALITY_ENCODING = "hbam.qseq-output.base-quality-encoding";
	public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "illumina";

	public static class QseqRecordWriter extends RecordWriter<Text,SequencedFragment>
	{
    protected static final byte[] newLine;
    protected static final String delim = "\t";
		static {
			try {
				newLine = "\n".getBytes("UTF-8");
			} catch (java.io.UnsupportedEncodingException e) {
				throw new RuntimeException("UTF-8 enconding not supported!");
			}
		}

		protected StringBuilder sBuilder = new StringBuilder(800);
		protected DataOutputStream out;
		BaseQualityEncoding baseQualityFormat;

    public QseqRecordWriter(Configuration conf, DataOutputStream out)
		{
			baseQualityFormat = BaseQualityEncoding.Illumina;
			this.out = out;
			setConf(conf);
		}

		public void setConf(Configuration conf)
		{
			String setting = conf.get(CONF_BASE_QUALITY_ENCODING, CONF_BASE_QUALITY_ENCODING_DEFAULT);
			if ("illumina".equals(setting))
				baseQualityFormat = BaseQualityEncoding.Illumina;
			else if ("sanger".equals(setting))
				baseQualityFormat = BaseQualityEncoding.Sanger;
			else
				throw new RuntimeException("Invalid property value '" + setting + "' for " + CONF_BASE_QUALITY_ENCODING + ".  Valid values are 'illumina' or 'sanger'");
		}

    public void write(Text ignored_key, SequencedFragment seq) throws IOException
		{
			sBuilder.delete(0, sBuilder.length()); // clear

			sBuilder.append( seq.getInstrument() == null ? "" : seq.getInstrument() ).append(delim);
			sBuilder.append( seq.getRunNumber() == null ? "" : seq.getRunNumber().toString() ).append(delim);
			sBuilder.append( seq.getLane() == null ? "" : seq.getLane().toString() ).append(delim);
			sBuilder.append( seq.getTile() == null ? "" : seq.getTile().toString() ).append(delim);
			sBuilder.append( seq.getXpos() == null ? "" : seq.getXpos().toString() ).append(delim);
			sBuilder.append( seq.getYpos() == null ? "" : seq.getYpos().toString() ).append(delim);
			sBuilder.append( seq.getIndexSequence() == null ? "" : seq.getIndexSequence().replace('N', '.') ).append(delim);
			sBuilder.append( seq.getRead() == null ? "" : seq.getRead().toString() ).append(delim);
			// here we also replace 'N' with '.'
			sBuilder.append( seq.getSequence() == null ? "" : seq.getSequence().toString().replace('N', '.')).append(delim);

			//////// quality may have to be re-coded
			if (seq.getQuality() == null)
				sBuilder.append("");
			else
			{
				int startPos = sBuilder.length();
				sBuilder.append(seq.getQuality().toString());
				if (baseQualityFormat == BaseQualityEncoding.Sanger)
				{
					//  do nothing
				}
				else if (baseQualityFormat == BaseQualityEncoding.Illumina)
				{
					// recode the quality in-place
					for (int i = startPos; i < sBuilder.length(); ++i)
					{
						// cast to avoid warning about possible loss of precision for assigning a char from an int.
						char newValue = (char)(sBuilder.charAt(i) + 31); // 64 - 33 = 31: difference between illumina and sanger encoding
						if (newValue > 126)
							throw new RuntimeException("output quality score over allowed range.  Maybe you meant to write in Sanger format?");
						sBuilder.setCharAt(i, newValue);
					}
				}
				else
					throw new RuntimeException("BUG!  Unknown base quality format value " + baseQualityFormat + " in QseqRecordWriter");
			}
			sBuilder.append(delim);
			/////////

			sBuilder.append((seq.getFilterPassed() == null || seq.getFilterPassed() ) ? 1 : 0);

			try {
				ByteBuffer buf = Text.encode(sBuilder.toString());
				out.write(buf.array(), 0, buf.limit());
			} catch (java.nio.charset.CharacterCodingException e) {
				throw new RuntimeException("Error encoding qseq record: " + seq);
			}
      out.write(newLine, 0, newLine.length);
    }

    public void close(TaskAttemptContext context) throws IOException
		{
      out.close();
    }
  }

  public RecordWriter<Text,SequencedFragment> getRecordWriter(TaskAttemptContext task)
	  throws IOException
	{
		Configuration conf = task.getConfiguration();
		boolean isCompressed = getCompressOutput(task);

		CompressionCodec codec = null;
		String extension = "";

		if (isCompressed)
		{
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(task, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}

		Path file = getDefaultWorkFile(task, extension);
		FileSystem fs = file.getFileSystem(conf);

		DataOutputStream output;

		if (isCompressed)
		{
			FSDataOutputStream fileOut = fs.create(file, false);
			output = new DataOutputStream(codec.createOutputStream(fileOut));
		}
		else
			output = fs.create(file, false);

		return new QseqRecordWriter(conf, output);
	}
}
