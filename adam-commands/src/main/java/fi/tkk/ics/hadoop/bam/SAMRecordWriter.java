// Copyright (c) 2010 Aalto University
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

// File created: 2012-02-23 12:42:49

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMTextWriter;
import parquet.hadoop.util.ContextUtil;

/** A base {@link RecordWriter} for SAM records.
 *
 * <p>Handles the output stream, writing the header if requested, and provides
 * the {@link #writeAlignment} function for subclasses.</p>
 */
public abstract class SAMRecordWriter<K>
	extends RecordWriter<K,SAMRecordWritable>
{
	private SAMTextWriter writer;
	private SAMFileHeader header;

	/** A SAMFileHeader is read from the input Path. */
	public SAMRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		final SAMFileReader r = new SAMFileReader(
			input.getFileSystem(ContextUtil.getConfiguration(ctx)).open(input));

		final SAMFileHeader hdr = r.getFileHeader();
		r.close();
		init(output, hdr, writeHeader, ctx);
	}
	public SAMRecordWriter(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output.getFileSystem(ContextUtil.getConfiguration(ctx)).create(output),
			header, writeHeader);
	}
	public SAMRecordWriter(
			OutputStream output, SAMFileHeader header, boolean writeHeader)
		throws IOException
	{
		init(output, header, writeHeader);
	}

	private void init(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		init(
			output.getFileSystem(ContextUtil.getConfiguration(ctx)).create(output),
			header, writeHeader);
	}
	private void init(
			OutputStream output, SAMFileHeader header, boolean writeHeader)
		throws IOException
	{
		this.header = header;
		writer = new SAMTextWriter(output);

		if (writeHeader)
			writer.setHeader(header);
	}

	@Override public void close(TaskAttemptContext ctx) {
		writer.close();
	}

	protected void writeAlignment(final SAMRecord rec) {
		rec.setHeader(header);
		writer.writeAlignment(rec);
	}
}
