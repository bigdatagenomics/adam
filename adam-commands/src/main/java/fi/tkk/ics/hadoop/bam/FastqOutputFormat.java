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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import parquet.hadoop.util.ContextUtil;

/**
 * Output format for the fastq format.
 *
 * If a key is provided with the SequencedFragment, the key is used as the sequence
 * id and the meta-info from the SequencedFragment (if any) is dropped.
 * If the key is null, then the format will attempt to create an 
 * Illumina-style fastq id as specified in the Casava users' guide v1.8:
 * @instrument:run number:flowcell ID:lane:tile:x-pos:y-pos \s+ read:is filtered:control number:index sequence
 *
 */
public class FastqOutputFormat extends TextOutputFormat<Text, SequencedFragment>
{
	public static final String CONF_BASE_QUALITY_ENCODING         = "hbam.fastq-output.base-quality-encoding";
	public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "sanger";

	protected BaseQualityEncoding baseQualityFormat = BaseQualityEncoding.Sanger;

	public static class FastqRecordWriter extends RecordWriter<Text,SequencedFragment>
	{
		protected StringBuilder       sBuilder          = new StringBuilder(800);
		protected Text                buffer            = new Text();
		protected DataOutputStream    out;
		protected BaseQualityEncoding baseQualityFormat;

    public FastqRecordWriter(Configuration conf, DataOutputStream out)
		{
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

		protected String makeId(SequencedFragment seq) throws IOException
		{
			String delim = ":";
			sBuilder.delete(0, sBuilder.length()); // clear

			sBuilder.append( seq.getInstrument() == null ? "" : seq.getInstrument() ).append(delim);
			sBuilder.append( seq.getRunNumber()  == null ? "" : seq.getRunNumber().toString() ).append(delim);
			sBuilder.append( seq.getFlowcellId()  == null ? "" : seq.getFlowcellId() ).append(delim);
			sBuilder.append( seq.getLane()       == null ? "" : seq.getLane().toString() ).append(delim);
			sBuilder.append( seq.getTile()       == null ? "" : seq.getTile().toString() ).append(delim);
			sBuilder.append( seq.getXpos()       == null ? "" : seq.getXpos().toString() ).append(delim);
			sBuilder.append( seq.getYpos()       == null ? "" : seq.getYpos().toString() );

			sBuilder.append(" "); // space

			sBuilder.append( seq.getRead()       == null ? "" : seq.getRead().toString() ).append(delim);
			sBuilder.append(seq.getFilterPassed() == null || seq.getFilterPassed() ? "N" : "Y");
			sBuilder.append(delim);

			sBuilder.append( seq.getControlNumber() == null ? "0" : seq.getControlNumber().toString()).append(delim);
			sBuilder.append( seq.getIndexSequence() == null ? "" : seq.getIndexSequence());

			return sBuilder.toString();
		}

    public void write(Text key, SequencedFragment seq) throws IOException
		{
			// write the id line
			out.writeByte('@');
			if (key != null)
				out.write(key.getBytes(), 0, key.getLength());
			else
				out.writeBytes(makeId(seq));
			out.writeByte('\n');

			// write the sequence and separator
			out.write(seq.getSequence().getBytes(), 0, seq.getSequence().getLength());
			out.writeBytes("\n+\n");

			// now the quality
			if (baseQualityFormat == BaseQualityEncoding.Sanger)
				out.write(seq.getQuality().getBytes(), 0, seq.getQuality().getLength());
			else if (baseQualityFormat == BaseQualityEncoding.Illumina)
			{
				buffer.set(seq.getQuality());
				SequencedFragment.convertQuality(buffer, BaseQualityEncoding.Sanger, baseQualityFormat);
				out.write(buffer.getBytes(), 0, buffer.getLength());
			}
			else
				throw new RuntimeException("FastqOutputFormat: unknown base quality format " + baseQualityFormat);

			// and the final newline
			out.writeByte('\n');
    }

    public void close(TaskAttemptContext task) throws IOException
		{
      out.close();
    }
  }

  public RecordWriter<Text,SequencedFragment> getRecordWriter(TaskAttemptContext task)
	  throws IOException
	{
		Configuration conf = ContextUtil.getConfiguration(task);
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

		return new FastqRecordWriter(conf, output);
	}
}
