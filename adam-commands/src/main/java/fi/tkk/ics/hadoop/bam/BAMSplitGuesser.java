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

// File created: 2011-01-17 15:17:59

package fi.tkk.ics.hadoop.bam;

import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.FileTruncatedException;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFormatException;
import net.sf.samtools.seekablestream.SeekableStream;
import net.sf.samtools.util.BlockCompressedInputStream;
import net.sf.samtools.util.RuntimeEOFException;

import fi.tkk.ics.hadoop.bam.util.SeekableArrayStream;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;
import parquet.hadoop.util.ContextUtil;

/** A class for heuristically finding BAM record positions inside an area of
 * a BAM file.
 */
public class BAMSplitGuesser {
	private       SeekableStream             inFile, in;
	private       BlockCompressedInputStream bgzf;
	private final BAMRecordCodec             bamCodec;
	private final ByteBuffer                 buf;
	private final int                        referenceSequenceCount;

	// We want to go through this many BGZF blocks fully, checking that they
	// contain valid BAM records, when guessing a BAM record position.
	private final static byte BLOCKS_NEEDED_FOR_GUESS = 2;

	// Since the max size of a BGZF block is 0xffff (64K), and we might be just
	// one byte off from the start of the previous one, we need 0xfffe bytes for
	// the start, and then 0xffff times the number of blocks we want to go
	// through.
	private final static int MAX_BYTES_READ =
		BLOCKS_NEEDED_FOR_GUESS * 0xffff + 0xfffe;

	private final static int BGZF_MAGIC     = 0x04088b1f;
	private final static int BGZF_MAGIC_SUB = 0x00024342;
	private final static int BGZF_SUB_SIZE  = 4 + 2;

	private final static int SHORTEST_POSSIBLE_BAM_RECORD = 4*9 + 1 + 1 + 1;

	/** The stream must point to a valid BAM file, because the header is read
	 * from it.
	 */
	public BAMSplitGuesser(SeekableStream ss) throws IOException {
		this(ss, ss);

		// Secondary check that the header points to a BAM file: Picard can get
		// things wrong due to its autodetection.
		ss.seek(0);
		if (ss.read(buf.array(), 0, 4) != 4 || buf.getInt(0) != BGZF_MAGIC)
			throw new SAMFormatException("Does not seem like a BAM file");
	}

	public BAMSplitGuesser(SeekableStream ss, InputStream headerStream)
		throws IOException
	{
		inFile = ss;

		buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.LITTLE_ENDIAN);

		referenceSequenceCount =
			new SAMFileReader(headerStream).getFileHeader()
			.getSequenceDictionary().size();

		bamCodec = new BAMRecordCodec(null, new LazyBAMRecordFactory());
	}

	/** Finds a virtual BAM record position in the physical position range
	 * [beg,end). Returns end if no BAM record was found.
	 */
	public long guessNextBAMRecordStart(long beg, long end)
		throws IOException
	{
		// Buffer what we need to go through.

		byte[] arr = new byte[MAX_BYTES_READ];

		this.inFile.seek(beg);
		int totalRead = 0;
		for (int left = Math.min((int)(end - beg), arr.length); left > 0;) {
			final int r = inFile.read(arr, totalRead, left);
			if (r < 0)
				break;
			totalRead += r;
			left -= r;
		}
		arr = Arrays.copyOf(arr, totalRead);

		this.in = new SeekableArrayStream(arr);

		this.bgzf = new BlockCompressedInputStream(this.in);
		this.bgzf.setCheckCrcs(true);

		this.bamCodec.setInputStream(bgzf);

		final int firstBGZFEnd = Math.min((int)(end - beg), 0xffff);

		// cp: Compressed Position, indexes the entire BGZF input.
		for (int cp = 0;; ++cp) {
			final PosSize psz = guessNextBGZFPos(cp, firstBGZFEnd);
			if (psz == null)
				return end;

			final int  cp0     = cp = psz.pos;
			final long cp0Virt = (long)cp0 << 16;
			try {
				bgzf.seek(cp0Virt);

			// This has to catch Throwable, because it's possible to get an
			// OutOfMemoryError due to an overly large size.
			} catch (Throwable e) {
				// Guessed BGZF position incorrectly: try the next guess.
				continue;
			}

			// up: Uncompressed Position, indexes the data inside the BGZF block.
			for (int up = 0;; ++up) {
				final int up0 = up = guessNextBAMPos(cp0Virt, up, psz.size);

				if (up0 < 0) {
					// No BAM records found in the BGZF block: try the next BGZF
					// block.
					break;
				}

				// Verify that we can actually decode BLOCKS_NEEDED_FOR_GUESS worth
				// of records starting at (cp0,up0).
				bgzf.seek(cp0Virt | up0);
				boolean decodedAny = false;
				try {
					byte b = 0;
					int prevCP = cp0;
					while (b < BLOCKS_NEEDED_FOR_GUESS && bamCodec.decode() != null)
					{
						decodedAny = true;

						final int cp2 = (int)(bgzf.getFilePointer() >>> 16);
						if (cp2 != prevCP) {
							// The compressed position changed so we must be in a new
							// block.
							assert cp2 > prevCP;
							prevCP = cp2;
							++b;
						}
					}

					// Running out of records to verify is fine as long as we
					// verified at least something. It should only happen if we
					// couldn't fill the array.
					if (b < BLOCKS_NEEDED_FOR_GUESS) {
						assert arr.length < MAX_BYTES_READ;
						if (!decodedAny)
							continue;
					}
				} catch (SAMFormatException     e) { continue; }
				  catch (FileTruncatedException e) { continue; }
				  catch (OutOfMemoryError       e) { continue; }
				  catch (RuntimeEOFException    e) {
					// This can happen legitimately if the [beg,end) range is too
					// small to accommodate BLOCKS_NEEDED_FOR_GUESS and we get cut
					// off in the middle of a record. In that case, our stream
					// should have hit EOF as well. If we've then verified at least
					// something, go ahead with it and hope for the best.
					if (!decodedAny && this.in.eof())
						continue;
				}

				return beg+cp0 << 16 | up0;
			}
		}
	}

	private static class PosSize {
		public int pos;
		public int size;
		public PosSize(int p, int s) { pos = p; size = s; }
	}

	// Gives the compressed size on the side. Returns null if it doesn't find
	// anything.
	private PosSize guessNextBGZFPos(int p, int end) {
		try { for (;;) {
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
					return null;
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

				// But find out the size before returning. First, grab bsize:
				// we'll need it later.
				in.read(buf.array(), 0, 2);
				int bsize = getUShort(0);

				// Then skip the rest of the subfields.
				p += BGZF_SUB_SIZE;
				while (p < subEnd) {
					in.seek(p);
					in.read(buf.array(), 0, 4);
					p += 4 + getUShort(2);
				}
				if (p != subEnd) {
					// Cancel our guess because the xlen field didn't match the
					// data.
					break;
				}

				// Now skip past the compressed data and the CRC-32.
				p += bsize - xlen - 19 + 4;
				in.seek(p);
				in.read(buf.array(), 0, 4);
				return new PosSize(p0, buf.getInt(0));
			}
			// No luck: look for the next gzip block header. Start right after
			// where we last saw the identifiers, although we could probably
			// safely skip further ahead. (If we find the correct one right
			// now, the previous block contained 0x1f8b0804 bytes of data: that
			// seems... unlikely.)
			p = p0 + 4;

		}} catch (IOException e) {
			return null;
		}
	}

	private int guessNextBAMPos(long cpVirt, int up, int cSize) {
		// What we're actually searching for is what's at offset [4], not [0]. So
		// skip ahead by 4, thus ensuring that whenever we find a valid [0] it's
		// at position up or greater.
		up += 4;

		try {
			while (up + SHORTEST_POSSIBLE_BAM_RECORD - 4 < cSize) {
				bgzf.seek(cpVirt | up);
				bgzf.read(buf.array(), 0, 8);

				// If the first two checks fail we have what looks like a valid
				// reference sequence ID. Assume we're at offset [4] or [24], i.e.
				// the ID of either this read or its mate, respectively. So check
				// the next integer ([8] or [28]) to make sure it's a 0-based
				// leftmost coordinate.
				final int id  = buf.getInt(0);
				final int pos = buf.getInt(4);
				if (id < -1 || id > referenceSequenceCount || pos < -1) {
					++up;
					continue;
				}

				// Okay, we could be at [4] or [24]. Assuming we're at [4], check
				// that [24] is valid. Assume [4] because we should hit it first:
				// the only time we expect to hit [24] is at the beginning of the
				// split, as part of the first read we should skip.

				bgzf.seek(cpVirt | up+20);
				bgzf.read(buf.array(), 0, 8);

				final int nid  = buf.getInt(0);
				final int npos = buf.getInt(4);
				if (nid < -1 || nid > referenceSequenceCount || npos < -1) {
					++up;
					continue;
				}

				// So far so good: [4] and [24] seem okay. Now do something a bit
				// more involved: make sure that [36 + [12]&0xff - 1] == 0: that
				// is, the name of the read should be null terminated.

				// Move up to 0 just to make it less likely that we get confused
				// with offsets. Remember where we should continue from if we
				// reject this up.
				final int nextUP = up + 1;
				up -= 4;

				bgzf.seek(cpVirt | up+12);
				bgzf.read(buf.array(), 0, 4);

				final int nameLength = buf.getInt(0) & 0xff;

				final int nullTerminator = up + 36 + nameLength-1;

				if (nullTerminator >= cSize) {
					// This BAM record can't fit here. But maybe there's another in
					// the remaining space, so try again.
					up = nextUP;
					continue;
				}

				bgzf.seek(cpVirt | nullTerminator);
				bgzf.read(buf.array(), 0, 1);

				if (buf.get(0) != 0) {
					up = nextUP;
					continue;
				}

				// All of [4], [24], and [36 + [12]&0xff] look good. If [0] is also
				// sensible, that's good enough for us. "Sensible" to us means the
				// following:
				//
				// [0] >= 4*([16]&0xffff) + [20] + ([20]+1)/2 + 4*8 + ([12]&0xff)

				// Note that [0] is "length of the _remainder_ of the alignment
				// record", which is why this uses 4*8 instead of 4*9.
				int zeroMin = 4*8 + nameLength;

				bgzf.seek(cpVirt | up+16);
				bgzf.read(buf.array(), 0, 8);

				zeroMin += (buf.getInt(0) & 0xffff) * 4;
				zeroMin += buf.getInt(4) + (buf.getInt(4)+1)/2;

				bgzf.seek(cpVirt | up);
				bgzf.read(buf.array(), 0, 4);

				if (buf.getInt(0) < zeroMin) {
					up = nextUP;
					continue;
				}
				return up;
			}
		} catch (IOException e) {}
		return -1;
	}
	private int getUShort(final int idx) {
		return (int)buf.getShort(idx) & 0xffff;
	}

	public static void main(String[] args) throws IOException {
		final GenericOptionsParser parser;
		try {
			parser = new GenericOptionsParser(args);

		// This should be IOException but Hadoop 0.20.2 doesn't throw it...
		} catch (Exception e) {
			System.err.printf("Error in Hadoop arguments: %s\n", e.getMessage());
			System.exit(1);

			// Hooray for javac
			return;
		}

		args = parser.getRemainingArgs();
		final Configuration conf = parser.getConfiguration();

		long beg = 0;

		if (args.length < 2 || args.length > 3) {
			System.err.println(
				"Usage: BAMSplitGuesser path-or-uri header-path-or-uri [beg]");
			System.exit(2);
		}

		try {
			if (args.length > 2) beg = Long.decode(args[2]);
		} catch (NumberFormatException e) {
			System.err.println("Invalid beg offset.");
			if (e.getMessage() != null)
				System.err.println(e.getMessage());
			System.exit(2);
		}

		SeekableStream ss = WrapSeekable.openPath(conf, new Path(args[0]));
		SeekableStream hs = WrapSeekable.openPath(conf, new Path(args[1]));

		final long end = beg + MAX_BYTES_READ;

		System.out.printf(
			"Will look for a BGZF block within: [%1$#x,%2$#x) = [%1$d,%2$d)\n"+
			"Will then verify BAM data within:  [%1$#x,%3$#x) = [%1$d,%3$d)\n",
			beg, beg + 0xffff, end);

		final long g =
			new BAMSplitGuesser(ss, hs).guessNextBAMRecordStart(beg, end);

		ss.close();

		if (g == end) {
			System.out.println(
				"Didn't find any acceptable BAM record in any BGZF block.");
			System.exit(1);
		}

		System.out.printf(
			"Accepted BGZF block at offset %1$#x (%1$d).\n"+
			"Accepted BAM record at offset %2$#x (%2$d) therein.\n",
			g >> 16, g & 0xffff);
	}
}
