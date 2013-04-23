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

// File created: 2010-08-20 13:54:10

package fi.tkk.ics.hadoop.bam.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;

import fi.tkk.ics.hadoop.bam.SAMFormat;

public final class GetSortedBAMHeader {
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println(
				"Usage: GetSortedBAMHeader input output\n\n"+

				"Reads the BAM header from input (a standard BGZF-compressed BAM "+
				"file), and\nwrites it (BGZF-compressed, no terminator block) to "+
				"output. Sets the sort order\nindicated in the SAM header to "+
				"'coordinate'.");
			System.exit(1);
		}

		final SAMFileHeader h =
			new SAMFileReader(new File(args[0])).getFileHeader();
		h.setSortOrder(SAMFileHeader.SortOrder.coordinate);

		new SAMOutputPreparer().prepareForRecords(
			new FileOutputStream(args[1]), SAMFormat.BAM, h);
	}
}
