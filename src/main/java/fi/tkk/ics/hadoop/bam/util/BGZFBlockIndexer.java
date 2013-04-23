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

// File created: 2010-08-25 11:28:28

package fi.tkk.ics.hadoop.bam.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;

/** An indexing tool for BGZF-compressed files, making them palatable to {@link
 * BGZFSplitFileInputFormat}. Writes BGZF block indices as understood by {@link
 * BGZFBlockIndex}.
 */
public final class BGZFBlockIndexer {
	public static void main(String[] args) {
		if (args.length <= 0) {
			System.out.println(
				"Usage: BGZFBlockIndexer GRANULARITY [BGZF files...]\n\n"+

				"Writes, for each GRANULARITY gzip blocks in a BGZF file, its "+
				"virtual file offset\nas a big-endian 48-bit integer into "+
				"[filename].bgzfi. The file is terminated by\nthe BGZF file's "+
				"length, in the same format.");
			return;
		}

		int granularity;
		try {
			granularity = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			granularity = 0;
		}
		if (granularity <= 0) {
			System.err.printf(
				"Granularity must be a positive integer, not '%s'!\n", args[0]);
			return;
		}

		final BGZFBlockIndexer indexer = new BGZFBlockIndexer(granularity);

		for (final String arg : Arrays.asList(args).subList(1, args.length)) {
			final File f = new File(arg);
			if (f.isFile() && f.canRead()) {
				System.out.printf("Indexing %s...", f);
				try {
					indexer.index(f);
					System.out.println(" done.");
				} catch (IOException e) {
					System.out.println(" FAILED!");
					e.printStackTrace();
				}
			} else
				System.err.printf(
					"%s does not look like a file, won't index!\n", f);
		}
	}

	private final ByteBuffer byteBuffer;
	private final int granularity;

	private int pos = 0;

	private static final int PRINT_EVERY = 500*1024*1024;

	public BGZFBlockIndexer(int g) {
		granularity = g;
		byteBuffer = ByteBuffer.allocate(8); // Enough to fit a long
	}

	private void index(final File file) throws IOException {
		final InputStream in = new FileInputStream(file);

		final OutputStream out = new BufferedOutputStream(
			new FileOutputStream(file.getPath() + ".bgzfi"));

		final LongBuffer lb =
			byteBuffer.order(ByteOrder.BIG_ENDIAN).asLongBuffer();

		long prevPrint = 0;
		pos = 0;

		for (int i = 0;;) {
			if (!skipBlock(in))
				break;

			if (++i == granularity) {
				i = 0;
				lb.put(0, pos);
				out.write(byteBuffer.array(), 2, 6);

				if (pos - prevPrint >= PRINT_EVERY) {
					System.out.print("-");
					prevPrint = pos;
				}
			}
		}
		lb.put(0, file.length());
		out.write(byteBuffer.array(), 2, 6);
		out.close();
		in.close();
	}

	private boolean skipBlock(final InputStream in) throws IOException {

		// Check magic number
		final int read = readBytes(in, 4);
		if (read != 4) {
			if (read == 0)
				return false;
			ioError("Invalid gzip header: too short, no ID/CM/FLG");
		}

		final int magic = byteBuffer.order(ByteOrder.BIG_ENDIAN).getInt(0);
		if (magic != 0x1f8b0804)
			ioError(
				"Invalid gzip header: bad ID/CM/FLG %#x != 0x1f8b0804", magic);

		// Skip to extra-length
		if (!readExactlyBytes(in, 8))
			ioError("Invalid gzip header: too short, no XLEN");

		byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

		final int xlen = getUshort(6);

		// Skip over each subfield until finding the one we care about
		for (int offset = 0; offset < xlen;) {
			if (!readExactlyBytes(in, 4))
				ioError("Invalid subfields: EOF after %d subfield bytes", offset);
			offset += 4;

			byteBuffer.order(ByteOrder.BIG_ENDIAN);
			final int siAndSlen = byteBuffer.getInt(0);
			byteBuffer.order(ByteOrder.LITTLE_ENDIAN);

			if ((siAndSlen & ~0xff) == 0x42430200) {
				if (!readExactlyBytes(in, 2))
					ioError("Invalid BGZF subfield: missing BSIZE");
				offset += 2;

				final int bsize = getUshort(0);

				// Skip over: rest of header + compressed data + rest of gzip block
				fullySkip(in, (xlen - offset) + (bsize - xlen - 19) + 8);
				pos += bsize + 1;
				return true;
			} else {
				final int slen = getUshort(2);
				fullySkip(in, slen);
				offset += slen;
			}
		}
		throw new IOException("Invalid BGZF file: block without BGZF subfield");
	}

	private int getUshort(final int idx) {
		return (int)byteBuffer.getShort(idx) & 0xffff;
	}

	private void fullySkip(final InputStream in, final int skip)
		throws IOException
	{
		// Skip repeatedly until we're either done skipping or can't skip any
		// more, in case some kind of IO error is temporarily preventing it. That
		// kind of situation might not necessarily be possible; the docs are
		// rather vague about the whole thing.
		for (int s = skip; s > 0;) {
			final long skipped = in.skip(s);
			if (skipped == 0)
				throw new IOException("Skip failed");
			s -= skipped;
		}
	}

	private int readBytes(final InputStream in, final int n)
		throws IOException
	{
		assert n <= byteBuffer.capacity();

		int read = 0;
		while (read < n) {
			final int readNow = in.read(byteBuffer.array(), read, n - read);
			if (readNow <= 0)
				break;
			read += readNow;
		}
		return read;
	}
	private boolean readExactlyBytes(final InputStream in, final int n)
		throws IOException
	{
		return readBytes(in, n) == n;
	}

	private void ioError(String s, Object... va) throws IOException {
		throw new IOException(String.format(s, va));
	}
}
