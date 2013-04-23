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

// File created: 2010-08-11 10:36:08

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMRecord;

/** A convenience class that you can use as a RecordWriter for BAM files.
 *
 * <p>The write function ignores the key, just outputting the SAMRecord.</p>
 */
public class KeyIgnoringBAMRecordWriter<K> extends BAMRecordWriter<K> {
	public KeyIgnoringBAMRecordWriter(
			Path output, Path input, boolean writeHeader, TaskAttemptContext ctx)
		throws IOException
	{
		super(output, input, writeHeader, ctx);
	}
	public KeyIgnoringBAMRecordWriter(
			Path output, SAMFileHeader header, boolean writeHeader,
			TaskAttemptContext ctx)
		throws IOException
	{
		super(output, header, writeHeader, ctx);
	}
	public KeyIgnoringBAMRecordWriter(
			OutputStream output, SAMFileHeader header, boolean writeHeader)
		throws IOException
	{
		super(output, header, writeHeader);
	}

	@Override public void write(K ignored, SAMRecordWritable rec) {
		writeAlignment(rec.get());
	}
}
