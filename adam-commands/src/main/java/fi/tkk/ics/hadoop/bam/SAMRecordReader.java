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

// File created: 2012-02-02 11:39:46

package fi.tkk.ics.hadoop.bam;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFormatException;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.SAMTextHeaderCodec;
import parquet.hadoop.util.ContextUtil;

/** See {@link BAMRecordReader} for the meaning of the key. */
public class SAMRecordReader
	extends RecordReader<LongWritable,SAMRecordWritable>
{
	private LongWritable key = new LongWritable();
	private SAMRecordWritable record = new SAMRecordWritable();

	private FSDataInputStream input;
	private SAMRecordIterator iterator;
	private long start, end;

   private WorkaroundingStream waInput;

	@Override public void initialize(InputSplit spl, TaskAttemptContext ctx)
		throws IOException
	{
		final FileSplit split = (FileSplit)spl;

		this.start =         split.getStart();
		this.end   = start + split.getLength();

		final Configuration conf = ContextUtil.getConfiguration(ctx);

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(conf);

		input = fs.open(file);

		// SAMFileReader likes to make our life difficult, so complexity ensues.
		// The basic problem is that SAMFileReader buffers its input internally,
		// which causes two issues.
		//
		// Issue #1 is that SAMFileReader requires that its input begins with a
		// SAM header. This is not fine for reading from the middle of a file.
		// Because of the buffering, if we have the reader read the header from
		// the beginning of the file and then seek to where we want to read
		// records from, it'll have buffered some records from immediately after
		// the header, which is no good. Thus we need to read the header
		// separately and then use a custom stream that wraps the input stream,
		// inserting the header at the beginning of it. (Note the spurious
		// re-encoding of the header so that the reader can decode it.)
		//
		// Issue #2 is handling the boundary between two input splits. The best
		// way seems to be the classic "in later splits, skip the first line, and
		// in every split finish reading a partial line at the end of the split",
		// but that latter part is a bit complicated here. Due to the buffering,
		// we can easily overshoot: as soon as the stream moves past the end of
		// the split, SAMFileReader has buffered some records past the end. The
		// basic fix here is to have our custom stream count the number of bytes
		// read and to stop after the split size. Unfortunately this prevents us
		// from reading the last partial line, so our stream actually allows
		// reading to the next newline after the actual end.

		final SAMFileHeader header =
			new SAMFileReader(input, false).getFileHeader();

		waInput = new WorkaroundingStream(input, header);

		final boolean firstSplit = this.start == 0;

		if (firstSplit) {
			// Skip the header because we already have it, and adjust the start to
			// match.
			final int headerLength = waInput.getRemainingHeaderLength();
			input.seek(headerLength);
			this.start += headerLength;
		} else
			input.seek(--this.start);

		// Creating the iterator causes reading from the stream, so make sure
		// to start counting this early.
		waInput.setLength(this.end - this.start);

		iterator = new SAMFileReader(waInput, false).iterator();

		if (!firstSplit) {
			// Skip the first line, it'll be handled with the previous split.
			try {
				if (iterator.hasNext())
					iterator.next();
			} catch (SAMFormatException e) {}
		}
	}
	@Override public void close() throws IOException { iterator.close(); }

	@Override public float getProgress() throws IOException {
		final long pos = input.getPos();
		if (pos >= end)
			return 1;
		else
			return (float)(pos - start) / (end - start);
	}
	@Override public LongWritable      getCurrentKey  () { return key; }
	@Override public SAMRecordWritable getCurrentValue() { return record; }

	@Override public boolean nextKeyValue() {
		if (!iterator.hasNext())
			return false;

		final SAMRecord r = iterator.next();
		key.set(BAMRecordReader.getKey(r));
		record.set(r);
		return true;
	}
}

// See the long comment in SAMRecordReader.initialize() for what this does.
class WorkaroundingStream extends InputStream {
	private final InputStream stream, headerStream;
	private boolean headerRemaining;
	private long length;
	private int headerLength;

	private boolean lookingForEOL = false,
	                foundEOL      = false,
	                strippingAts  = false; // HACK, see read(byte[], int, int).

	public WorkaroundingStream(InputStream stream, SAMFileHeader header) {
		this.stream = stream;

		String text = header.getTextHeader();
		if (text == null) {
			StringWriter writer = new StringWriter();
			new SAMTextHeaderCodec().encode(writer, header);
			text = writer.toString();
		}
		byte[] b;
		try {
			b = text.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			b = null;
			assert false;
		}
		headerRemaining = true;
		headerLength    = b.length;
		headerStream    = new ByteArrayInputStream(b);

		this.length = Long.MAX_VALUE;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public int getRemainingHeaderLength() {
		return headerLength;
	}

	private byte[] readBuf = new byte[1];
	@Override public int read() throws IOException {
		for (;;) switch (read(readBuf)) {
			case  0: continue;
			case  1: return readBuf[0];
			case -1: return -1;
		}
	}

	@Override public int read(byte[] buf, int off, int len) throws IOException {
		if (!headerRemaining)
			return streamRead(buf, off, len);

		int h;
		if (strippingAts)
			h = 0;
		else {
			h = headerStream.read(buf, off, len);
			if (h == -1) {
				// This should only happen when there was no header at all, in
				// which case Picard doesn't throw an error until trying to read
				// a record, for some reason. (Perhaps an oversight.) Thus we
				// need to handle that case here.
				assert (headerLength == 0);
				h = 0;
			} else if (h < headerLength) {
				headerLength -= h;
				return h;
			}
			strippingAts = true;
			headerStream.close();
		}

		final int newOff = off + h;
		int s = streamRead(buf, newOff, len - h);

		if (s <= 0)
			return strippingAts ? s : h;

		// HACK HACK HACK.
		//
		// We gave all of the header, which means that SAMFileReader is still
		// trying to read more header lines. If we're in a split that isn't at
		// the start of the SAM file, we could be in the middle of a line and
		// thus see @ characters at the start of our data. Then SAMFileReader
		// would try to understand those as header lines and the end result is
		// that it throws an error, since they aren't actually header lines,
		// they're just part of a SAM record.
		//
		// So, if we're done with the header, strip all @ characters we see. Thus
		// SAMFileReader will stop reading the header there and won't throw an
		// exception until we use its SAMRecordIterator, at which point we can
		// catch it, because we know to expect it.
		//
		// headerRemaining remains true while it's possible that there are still
		// @ characters coming.

		int i = newOff-1;
		while (buf[++i] == '@' && --s > 0);

		if (i != newOff)
			System.arraycopy(buf, i, buf, newOff, s);

		headerRemaining = s == 0;
		return h + s;
	}
	private int streamRead(byte[] buf, int off, int len) throws IOException {
		if (len > length) {
			if (foundEOL)
				return 0;
			lookingForEOL = true;
		}
		int n = stream.read(buf, off, len);
		if (n > 0) {
			n = tryFindEOL(buf, off, n);
			length -= n;
		}
		return n;
	}
	private int tryFindEOL(byte[] buf, int off, int len) {
		assert !foundEOL;

		if (!lookingForEOL || len < length)
			return len;

		// Find the first EOL between length and len.

		// len >= length so length fits in an int.
		int i = Math.max(0, (int)length - 1);

		for (; i < len; ++i) {
			if (buf[off + i] == '\n') {
				foundEOL = true;
				return i + 1;
			}
		}
		return len;
	}

	@Override public void close() throws IOException {
		stream.close();
	}

	@Override public int available() throws IOException {
		return headerRemaining ? headerStream.available() : stream.available();
	}
}
