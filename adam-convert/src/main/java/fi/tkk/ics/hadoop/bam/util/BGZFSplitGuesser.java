// Copyright (c) 2011 Aalto University
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

// File created: 2011-05-31 11:40:06

package fi.tkk.ics.hadoop.bam.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;

import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.RuntimeEOFException;

import fi.tkk.ics.hadoop.bam.util.SeekableArrayStream;

public class BGZFSplitGuesser {
	private       FSDataInputStream inFile;
	private       SeekableArrayStream in;
	private final ByteBuffer buf;

	private final static int BGZF_MAGIC     = 0x04088b1f;
	private final static int BGZF_MAGIC_SUB = 0x00024342;
	private final static int BGZF_SUB_SIZE  = 4 + 2;

	public BGZFSplitGuesser(FSDataInputStream is) {
		inFile = is;

		buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.LITTLE_ENDIAN);
	}

	/// Looks in the range [beg,end). Returns end if no BAM record was found.
	public long guessNextBGZFBlockStart(long beg, long end)
		throws IOException
	{
		// Buffer what we need to go through. Since the max size of a BGZF block
		// is 0xffff (64K), and we might be just one byte off from the start of
		// the previous one, we need 0xfffe bytes for the start, and then 0xffff
		// for the block we're looking for.

		byte[] arr = new byte[2*0xffff - 1];

		this.inFile.seek(beg);
		arr = Arrays.copyOf(arr, inFile.read(arr, 0, Math.min((int)(end - beg),
		                                                      arr.length)));

		this.in = new SeekableArrayStream(arr);

		final BlockCompressedInputStream bgzf =
			new BlockCompressedInputStream(this.in);
		bgzf.setCheckCrcs(true);

		final int firstBGZFEnd = Math.min((int)(end - beg), 0xffff);

		for (int pos = 0;;) {
			pos = guessNextBGZFPos(pos, firstBGZFEnd);
			if (pos < 0)
				return end;

			try {
				// Seek in order to trigger decompression of the block and a CRC
				// check.
				bgzf.seek((long)pos << 16);

			// This has to catch Throwable, because it's possible to get an
			// OutOfMemoryError due to an overly large size.
			} catch (Throwable e) {
				// Guessed BGZF position incorrectly: try the next guess.
				++pos;
				continue;
			}
			return beg + pos;
		}
	}

	// Returns a negative number if it doesn't find anything.
	private int guessNextBGZFPos(int p, int end)
		throws IOException
	{
		for (;;) {
			for (;;) {
				in.seek(p);
				in.read(buf.array(), 0, 4);
				int n = buf.getInt(0);

				if (n == BGZF_MAGIC)
					break;

				// Skip ahead a bit more than 1 byte if you can.
				if (n >>> 8 == BGZF_MAGIC << 8 >>> 8)
					++p;
				else if (n >>> 16 == BGZF_MAGIC << 16 >>> 16)
					p += 2;
				else
					p += 3;

				if (p >= end)
					return -1;
			}
			// Found what looks like a gzip block header: now get XLEN and
			// search for the BGZF subfield.
			final int p0 = p;
			p += 10;
			in.seek(p);
			in.read(buf.array(), 0, 2);
			p += 2;
			final int xlen   = getUShort(0);
			final int subEnd = p + xlen;

			while (p < subEnd) {
				in.read(buf.array(), 0, 4);

				if (buf.getInt(0) != BGZF_MAGIC_SUB) {
					p += 4 + getUShort(2);
					in.seek(p);
					continue;
				}

				// Found it: this is close enough to a BGZF block, make it
				// our guess.
				return p0;
			}
			// No luck: look for the next gzip block header. Start right after
			// where we last saw the identifiers, although we could probably
			// safely skip further ahead. (If we find the correct one right
			// now, the previous block contained 0x1f8b0804 bytes of data: that
			// seems... unlikely.)
			p = p0 + 4;
		}
	}

	private int getUShort(final int idx) {
		return (int)buf.getShort(idx) & 0xffff;
	}
}
