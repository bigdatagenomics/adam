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

// File created: 2010-08-12 09:57:45

package fi.tkk.ics.hadoop.bam;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import net.sf.samtools.BAMRecordCodec;
import net.sf.samtools.SAMRecord;

import fi.tkk.ics.hadoop.bam.util.DataInputWrapper;
import fi.tkk.ics.hadoop.bam.util.DataOutputWrapper;

/** A {@link Writable} {@link SAMRecord}.
 *
 * <p>In every mapper, the record will have a header, since BAMInputFormat
 * provides one. It is lost when transferring the SAMRecord to a reducer,
 * however. The current implementation of {@link BAMRecordCodec} does not
 * require a record for encoding nor decoding of a <code>SAMRecord</code>, so
 * this fortunately doesn't matter for either {@link #write} or {@link
 * #readFields}.</p>
 */
public class SAMRecordWritable implements Writable {
	private static final BAMRecordCodec lazyCodec =
		new BAMRecordCodec(null, new LazyBAMRecordFactory());

	private SAMRecord record;

	public SAMRecord get()            { return record; }
	public void      set(SAMRecord r) { record = r; }

	@Override public void write(DataOutput out) throws IOException {
		// In theory, it shouldn't matter whether we give a header to
		// BAMRecordCodec or not, since the representation of an alignment in BAM
		// doesn't depend on the header data at all. Only its interpretation
		// does, and a simple read/write codec shouldn't really have anything to
		// say about that. (But in practice, it already does matter for decode(),
		// which is why LazyBAMRecordFactory exists.)
		final BAMRecordCodec codec = new BAMRecordCodec(record.getHeader());
		codec.setOutputStream(new DataOutputWrapper(out));
		codec.encode(record);
	}
	@Override public void readFields(DataInput in) throws IOException {
		lazyCodec.setInputStream(new DataInputWrapper(in));
		record = lazyCodec.decode();
	}
}
