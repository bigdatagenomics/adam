// Copyright (c) 2012 Aalto University
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

// File created: 2012-07-26 14:36:03

package fi.tkk.ics.hadoop.bam.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMTextHeaderCodec;
import net.sf.samtools.util.BlockCompressedOutputStream;

import fi.tkk.ics.hadoop.bam.SAMFormat;

public class SAMOutputPreparer {
	private ByteBuffer buf;

	public SAMOutputPreparer() {
		// Enough room for a 32-bit integer.
		buf = ByteBuffer.wrap(new byte[4]);
		buf.order(ByteOrder.LITTLE_ENDIAN);
	}

	public static final byte[] BAM_MAGIC = {'B','A','M', 1};

	/** Prepares the given output stream for writing of SAMRecords in the given
	 * format. This includes writing the given SAM header and, in the case of
	 * BAM, writing some further metadata as well as compressing everything
	 * written. Returns a new stream to replace the original: it will do the
	 * appropriate compression for BAM files.
	 */
	public OutputStream prepareForRecords(
			OutputStream out, final SAMFormat format,
			final SAMFileHeader header)
		throws IOException
	{
		final StringWriter sw = new StringWriter();
		new SAMTextHeaderCodec().encode(sw, header);
		final String text = sw.toString();

		if (format == SAMFormat.BAM) {
			out = new BlockCompressedOutputStream(out, null);
			out.write(BAM_MAGIC);
			writeInt32(out, text.length());
		}

		writeString(out, text);

		if (format == SAMFormat.BAM) {
			final List<SAMSequenceRecord> refs =
				header.getSequenceDictionary().getSequences();

			writeInt32(out, refs.size());

			for (final SAMSequenceRecord ref : refs) {
				final String name = ref.getSequenceName();
				writeInt32(out, name.length() + 1);
				writeString(out, name);
				out.write(0);
				writeInt32(out, ref.getSequenceLength());
			}
		}

		// Important for BAM: if the caller doesn't want to use the new stream
		// for some reason, the BlockCompressedOutputStream's buffer would never
		// be flushed.
		out.flush();
		return out;
	}

	private static void writeString(final OutputStream out, final String s)
		throws IOException
	{
		// Don't flush the underlying stream yet, only the writer: in the case of
		// BAM, we might be able to cram more things into the gzip block still.
		final OutputStreamWriter w = new OutputStreamWriter(
			new FilterOutputStream(out) { @Override public void flush() {} } );
		w.write(s);
		w.flush();
	}

	private void writeInt32(final OutputStream out, int n) throws IOException {
		buf.putInt(0, n);
		out.write(buf.array());
	}
}
