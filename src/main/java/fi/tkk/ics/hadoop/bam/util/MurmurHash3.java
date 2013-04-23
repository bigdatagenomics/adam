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

package fi.tkk.ics.hadoop.bam.util;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.ByteOrder;

/** This class implements a hash function giving the first 64 bits of the
 * MurmurHash3_x64_128 hash.
 */
@SuppressWarnings("fallthrough")
public final class MurmurHash3 {
	public static long murmurhash3(byte[] key, int seed) {

		final ByteBuffer data =
			ByteBuffer.wrap(key).order(ByteOrder.LITTLE_ENDIAN);

		final int len = key.length;

		final int nblocks = len / 16;

		long h1 = seed;
		long h2 = seed;

		final long c1 = 0x87c37b91114253d5L;
		final long c2 = 0x4cf5ad432745937fL;

		final LongBuffer blocks = data.asLongBuffer();

		for (int i = 0; i < nblocks; ++i) {
			long k1 = blocks.get(i*2 + 0);
			long k2 = blocks.get(i*2 + 1);

			k1 *= c1; k1 = k1 << 31 | k1 >>> (64-31); k1 *= c2; h1 ^= k1;

			h1 = h1 << 27 | h1 >>> (64-27); h1 += h2; h1 = h1*5 + 0x52dce729;

			k2 *= c2; k2 = k2 << 33 | k2 >>> (64-33); k2 *= c1; h2 ^= k2;

			h2 = h2 << 31 | h1 >>> (64-31); h2 += h1; h2 = h2*5 + 0x38495ab5;
		}

		data.position(nblocks * 16);
		final ByteBuffer tail = data.slice();

		long k1 = 0;
		long k2 = 0;

		switch (len & 15) {
			case 15: k2 ^= ((long)tail.get(14) & 0xff) << 48;
			case 14: k2 ^= ((long)tail.get(13) & 0xff) << 40;
			case 13: k2 ^= ((long)tail.get(12) & 0xff) << 32;
			case 12: k2 ^= ((long)tail.get(11) & 0xff) << 24;
			case 11: k2 ^= ((long)tail.get(10) & 0xff) << 16;
			case 10: k2 ^= ((long)tail.get( 9) & 0xff) << 8;
			case  9: k2 ^= ((long)tail.get( 8) & 0xff) << 0;
						k2 *= c2; k2 = k2 << 33 | k2 >>> (64-33); k2 *= c1; h2 ^= k2;

			case  8: k1 ^= ((long)tail.get( 7) & 0xff) << 56;
			case  7: k1 ^= ((long)tail.get( 6) & 0xff) << 48;
			case  6: k1 ^= ((long)tail.get( 5) & 0xff) << 40;
			case  5: k1 ^= ((long)tail.get( 4) & 0xff) << 32;
			case  4: k1 ^= ((long)tail.get( 3) & 0xff) << 24;
			case  3: k1 ^= ((long)tail.get( 2) & 0xff) << 16;
			case  2: k1 ^= ((long)tail.get( 1) & 0xff) << 8;
			case  1: k1 ^= ((long)tail.get( 0) & 0xff) << 0;
						k1 *= c1; k1 = k1 << 31 | k1 >>> (64-31); k1 *= c2; h1 ^= k1;
			case  0: break;
		}

		h1 ^= len; h2 ^= len;

		h1 += h2;
		h2 += h1;

		h1 = fmix(h1);
		h2 = fmix(h2);

		h1 += h2;
		// h2 += h1;

		return h1;
	}

	/** This version hashes the characters directly. It is not equivalent to
	 * hashing chars.toString().getBytes(), as it hashes UTF-16 code units, but
	 * it is much faster.
	 */
	public static long murmurhash3(CharSequence chars, int seed) {

		final int len = chars.length();

		final int nblocks = len / 8;

		long h1 = seed;
		long h2 = seed;

		final long c1 = 0x87c37b91114253d5L;
		final long c2 = 0x4cf5ad432745937fL;

		for (int i = 0; i < nblocks; ++i) {
			int i0 = (i*2 + 0) * 4;
			int i1 = (i*2 + 1) * 4;

			long k1 = (long)chars.charAt(i0)
				     | (long)chars.charAt(i0+1) << 16
				     | (long)chars.charAt(i0+2) << 32
				     | (long)chars.charAt(i0+3) << 48;
			long k2 = (long)chars.charAt(i1)
				     | (long)chars.charAt(i1+1) << 16
				     | (long)chars.charAt(i1+2) << 32
				     | (long)chars.charAt(i1+3) << 48;

			k1 *= c1; k1 = k1 << 31 | k1 >>> (64-31); k1 *= c2; h1 ^= k1;

			h1 = h1 << 27 | h1 >>> (64-27); h1 += h2; h1 = h1*5 + 0x52dce729;

			k2 *= c2; k2 = k2 << 33 | k2 >>> (64-33); k2 *= c1; h2 ^= k2;

			h2 = h2 << 31 | h1 >>> (64-31); h2 += h1; h2 = h2*5 + 0x38495ab5;
		}

		long k1 = 0;
		long k2 = 0;

		switch (len & 7) {
			case  7: k2 ^= (long)chars.charAt(6) << 32;
			case  6: k2 ^= (long)chars.charAt(5) << 16;
			case  5: k2 ^= (long)chars.charAt(4) << 0;
						k2 *= c2; k2 = k2 << 33 | k2 >>> (64-33); k2 *= c1; h2 ^= k2;

			case  4: k1 ^= (long)chars.charAt(3) << 48;
			case  3: k1 ^= (long)chars.charAt(2) << 32;
			case  2: k1 ^= (long)chars.charAt(1) << 16;
			case  1: k1 ^= (long)chars.charAt(0) << 0;
						k1 *= c1; k1 = k1 << 31 | k1 >>> (64-31); k1 *= c2; h1 ^= k1;
			case  0: break;
		}

		h1 ^= len; h2 ^= len;

		h1 += h2;
		h2 += h1;

		h1 = fmix(h1);
		h2 = fmix(h2);

		h1 += h2;
		// h2 += h1;

		return h1;
	}

	private static long fmix(long k) {
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;
		return k;
	}
}
