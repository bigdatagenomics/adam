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

// File created: 2010-08-04 13:11:10

package fi.tkk.ics.hadoop.bam;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.TreeSet;

/** An index into BAM files, for {@link BAMInputFormat}. Reads files that are
 * created by {@link SplittingBAMIndexer}.
 *
 * <p>Indexes the positions of individual BAM records in the file.</p>
 */
public final class SplittingBAMIndex {
	private final NavigableSet<Long> virtualOffsets = new TreeSet<Long>();

	public SplittingBAMIndex() {}
	public SplittingBAMIndex(final File path) throws IOException {
		this(new BufferedInputStream(new FileInputStream(path)));
	}
	public SplittingBAMIndex(final InputStream in) throws IOException {
		readIndex(in);
	}

	public void readIndex(final InputStream in) throws IOException {
		virtualOffsets.clear();

		final ByteBuffer bb = ByteBuffer.allocate(8);

		for (long prev = -1; in.read(bb.array()) == 8;) {
			final long cur = bb.getLong(0);
			if (prev > cur)
				throw new IOException(String.format(
					"Invalid splitting BAM index; offsets not in order: %#x > %#x",
					prev, cur));

			virtualOffsets.add(prev = cur);
		}
		in.close();

		if (virtualOffsets.size() < 2)
			throw new IOException(
				"Invalid splitting BAM index: "+
				"should contain at least 1 offset and the file size");
	}

	public Long prevAlignment(final long filePos) {
		return virtualOffsets.floor(filePos << 16);
	}
	public Long nextAlignment(final long filePos) {
		return virtualOffsets.higher(filePos << 16);
	}

	public int size() { return virtualOffsets.size(); }

	private long   first() { return virtualOffsets.first(); }
	private long    last() { return prevAlignment(bamSize() - 1); }
	private long bamSize() { return virtualOffsets.last() >>> 16; }

	/** Writes some statistics about each splitting BAM index file given as an
	 * argument.
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println(
				"Usage: SplittingBAMIndex [splitting BAM indices...]\n\n"+

				"Writes a few statistics about each splitting BAM index.");
			return;
		}

		for (String arg : args) {
			final File f = new File(arg);
			if (f.isFile() && f.canRead()) {
				try {
					System.err.printf("%s:\n", f);
					final SplittingBAMIndex bi = new SplittingBAMIndex(f);
					final long first = bi.first();
					final long last  = bi.last();
					System.err.printf(
						"\t%d alignments\n" +
						"\tfirst is at %#06x in BGZF block at %#014x\n" +
						"\tlast  is at %#06x in BGZF block at %#014x\n" +
						"\tassociated BAM file size %d\n",
						bi.size(),
						first & 0xffff, first >>> 16,
						last  & 0xffff, last  >>> 16,
						bi.bamSize());
				} catch (IOException e) {
					System.err.printf("Failed to read %s!\n", f);
					e.printStackTrace();
				}
			} else
				System.err.printf("%s does not look like a readable file!\n", f);
		}
	}
}
