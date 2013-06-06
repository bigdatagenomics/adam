// Copyright (C) 2011-2012 CRS4.
//
// This file is part of Hadoop-BAM.
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

package fi.tkk.ics.hadoop.bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;

public class FormatConstants
{
	/**
	 * Offset by which Sanger-style ASCII-encoded quality scores are shifted.
	 */
	public static final int SANGER_OFFSET = 33;

	/**
	 * Maximum encodable quality score for Sanger Phred+33 encoded base qualities.
	 */
	public static final int SANGER_MAX = 62;

	/**
	 * Offset by which Illumina-style ASCII-encoded quality scores are shifted.
	 */
	public static final int ILLUMINA_OFFSET = 64;

	/**
	 * Maximum encodable quality score for Illumina Phred+64 encoded base qualities.
	 */
	public static final int ILLUMINA_MAX = 62;

	/**
	 * Encodings for base quality formats.
	 */
	public enum BaseQualityEncoding { Illumina, Sanger };

	private FormatConstants() {} // no instantiation
}
