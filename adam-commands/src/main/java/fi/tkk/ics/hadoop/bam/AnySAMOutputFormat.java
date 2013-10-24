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

// File created: 2012-02-23 13:00:24

package fi.tkk.ics.hadoop.bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** An abstract {@link org.apache.hadoop.mapreduce.OutputFormat} for SAM and
 * BAM files. Only locks down the value type and stores the output format
 * requested.
 */
public abstract class AnySAMOutputFormat<K>
	extends FileOutputFormat<K,SAMRecordWritable>
{
	/** A string property defining the output format to use. The value is read
	 * directly by {@link SAMFormat#valueOf}.
	 */
	public static final String OUTPUT_SAM_FORMAT_PROPERTY =
		"hadoopbam.anysam.output-format";

	protected SAMFormat format;

	/** Creates a new output format, reading {@link #OUTPUT_SAM_FORMAT_PROPERTY}
	 * from the given <code>Configuration</code>.
	 */
	protected AnySAMOutputFormat(Configuration conf) {
		final String fmtStr = conf.get(OUTPUT_SAM_FORMAT_PROPERTY);

		format = fmtStr == null ? null : SAMFormat.valueOf(fmtStr);
	}

	/** Creates a new output format for the given SAM format. */
	protected AnySAMOutputFormat(SAMFormat fmt) {
		if (fmt == null)
			throw new IllegalArgumentException("null SAMFormat");
		format = fmt;
	}
}
