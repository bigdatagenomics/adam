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

// File created: 2010-08-25 12:20:03

package fi.tkk.ics.hadoop.bam.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.TreeSet;

/** An index into BGZF-compressed files, for {@link BGZFSplitFileInputFormat}.
 * Reads files that are created by {@link BGZFBlockIndexer}.
 *
 * <p>Indexes the positions of individual gzip blocks in the file.</p>
 */
public final class BGZFBlockIndex {
	private final NavigableSet<Long> offsets = new TreeSet<Long>();

	public BGZFBlockIndex() {}
	public BGZFBlockIndex(final File path) throws IOException {
		this(new BufferedInputStream(new FileInputStream(path)));
	}
	public BGZFBlockIndex(final InputStream in) throws IOException {
		readIndex(in);
	}

	public void readIndex(final InputStream in) throws IOException {
		offsets.clear();

		final ByteBuffer bb = ByteBuffer.allocate(8);

		for (long prev = -1; in.read(bb.array(), 2, 6) == 6;) {
			final long cur = bb.getLong(0);
			if (prev > cur)
				throw new IOException(String.format(
					"Invalid BGZF block index; offsets not in order: %#x > %#x",
					prev, cur));

			offsets.add(prev = cur);
		}
		in.close();

		if (offsets.size() < 1)
			throw new IOException(
				"Invalid BGZF block index: should contain at least the file size");

		offsets.add(0L);
	}

	public Long prevBlock(final long filePos) {
		return offsets.floor(filePos);
	}
	public Long nextBlock(final long filePos) {
		return offsets.higher(filePos);
	}

	public int size() { return offsets.size(); }

	private long secondBlock() { return nextBlock(0); }
	private long   lastBlock() { return prevBlock(fileSize() - 1); }
	private long    fileSize() { return offsets.last(); }

	/** Writes some statistics about each BGZF block index file given as an
	 * argument.
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println(
				"Usage: BGZFBlockIndex [BGZF block indices...]\n\n"+

				"Writes a few statistics about each BGZF block index.");
			return;
		}

		for (String arg : args) {
			final File f = new File(arg);
			if (f.isFile() && f.canRead()) {
				try {
					System.err.printf("%s:\n", f);
					final BGZFBlockIndex bi = new BGZFBlockIndex(f);
					final long second = bi.secondBlock();
					final long last   = bi.lastBlock();
					System.err.printf(
						"\t%d blocks\n" +
						"\tfirst after 0 is at %#014x\n" +
						"\tlast          is at %#014x\n" +
						"\tassociated BGZF file size %d\n",
						bi.size()-1,
						bi.secondBlock(), bi.lastBlock(), bi.fileSize());
				} catch (IOException e) {
					System.err.printf("Failed to read %s!\n", f);
					e.printStackTrace();
				}
			} else
				System.err.printf("%s does not look like a readable file!\n", f);
		}
	}
}
