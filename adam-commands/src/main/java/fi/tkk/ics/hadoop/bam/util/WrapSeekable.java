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

// File created: 2010-08-25 11:24:30

package fi.tkk.ics.hadoop.bam.util;

import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import net.sf.samtools.seekablestream.SeekableStream;

/** Wraps Hadoop's "seekable stream" abstraction so that we can give such a one
 * to BlockCompressedInputStream and retain seekability.
 *
 * <p>This is necessary because Hadoop and the SAM tools each have their own
 * "seekable stream" abstraction.</p>
 */
public class WrapSeekable<S extends InputStream & Seekable>
	extends SeekableStream
{
	private final S    stm;
	private final long len;
	private final Path path;

	public WrapSeekable(final S s, long length, Path p) {
		stm  = s;
		len  = length;
		path = p;
	}

	/** A helper for the common use case. */
	public static WrapSeekable<FSDataInputStream> openPath(
		FileSystem fs, Path p) throws IOException
	{
		return new WrapSeekable<FSDataInputStream>(
			fs.open(p), fs.getFileStatus(p).getLen(), p);
	}
	public static WrapSeekable<FSDataInputStream> openPath(
		Configuration conf, Path path) throws IOException
	{
		return openPath(path.getFileSystem(conf), path);
	}

	@Override public String getSource() { return path.toString(); }
	@Override public long   length   () { return len; }

	@Override public long position() throws IOException { return stm.getPos(); }
	@Override public void    close() throws IOException { stm.close(); }
	@Override public boolean eof  () throws IOException {
		return stm.getPos() == length();
	}
	@Override public void seek(long pos) throws IOException {
		stm.seek(pos);
	}
	@Override public int read() throws IOException {
		return stm.read();
	}
	@Override public int read(byte[] buf, int offset, int len)
		throws IOException
	{
		return stm.read(buf, offset, len);
	}
}
