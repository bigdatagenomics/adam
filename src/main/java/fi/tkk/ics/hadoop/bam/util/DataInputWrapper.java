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

// File created: 2011-06-27 09:25:59

package fi.tkk.ics.hadoop.bam.util;

import java.io.DataInput;
import java.io.InputStream;
import java.io.IOException;

public class DataInputWrapper extends InputStream {
	private final DataInput in;

	public DataInputWrapper(DataInput i) { in = i; }

	@Override public long skip(long n) throws IOException {
		for (; n > Integer.MAX_VALUE; n -= Integer.MAX_VALUE) {
			final int skipped = in.skipBytes(Integer.MAX_VALUE);
			if (skipped < Integer.MAX_VALUE)
				return skipped;
		}
		return in.skipBytes((int)n);
	}
	@Override public int read(byte[] b, int off, int len) throws IOException {
		in.readFully(b, off, len);
		return len;
	}
	@Override public int read() throws IOException {
		return in.readByte();
	}
}
