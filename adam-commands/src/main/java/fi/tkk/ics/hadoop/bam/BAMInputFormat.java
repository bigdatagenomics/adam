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

// File created: 2010-08-03 11:50:19

package fi.tkk.ics.hadoop.bam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import net.sf.samtools.util.SeekableStream;

import fi.tkk.ics.hadoop.bam.util.WrapSeekable;
import parquet.hadoop.util.ContextUtil;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for BAM files. Values
 * are the individual records; see {@link BAMRecordReader} for the meaning of
 * the key.
 */
public class BAMInputFormat
	extends FileInputFormat<LongWritable,SAMRecordWritable>
{
	private Path getIdxPath(Path path) { return path.suffix(".splitting-bai"); }

	/** Returns a {@link BAMRecordReader} initialized with the parameters. */
	@Override public RecordReader<LongWritable,SAMRecordWritable>
		createRecordReader(InputSplit split, TaskAttemptContext ctx)
			throws InterruptedException, IOException
	{
		final RecordReader<LongWritable,SAMRecordWritable> rr =
			new BAMRecordReader();
		rr.initialize(split, ctx);
		return rr;
	}

	/** The splits returned are {@link FileVirtualSplit FileVirtualSplits}. */
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		return getSplits(super.getSplits(job), ContextUtil.getConfiguration(job));
	}

	public List<InputSplit> getSplits(
			List<InputSplit> splits, Configuration cfg)
		throws IOException
	{
		// Align the splits so that they don't cross blocks.

		// addIndexedSplits() requires the given splits to be sorted by file
		// path, so do so. Although FileInputFormat.getSplits() does, at the time
		// of writing this, generate them in that order, we shouldn't rely on it.
		Collections.sort(splits, new Comparator<InputSplit>() {
			public int compare(InputSplit a, InputSplit b) {
				FileSplit fa = (FileSplit)a, fb = (FileSplit)b;
				return fa.getPath().compareTo(fb.getPath());
			}
		});

		final List<InputSplit> newSplits =
			new ArrayList<InputSplit>(splits.size());

		for (int i = 0; i < splits.size();) {
			try {
				i = addIndexedSplits      (splits, i, newSplits, cfg);
			} catch (IOException e) {
				i = addProbabilisticSplits(splits, i, newSplits, cfg);
			}
		}
		return newSplits;
	}

	// Handles all the splits that share the Path of the one at index i,
	// returning the next index to be used.
	private int addIndexedSplits(
			List<InputSplit> splits, int i, List<InputSplit> newSplits,
			Configuration cfg)
		throws IOException
	{
		final Path file = ((FileSplit)splits.get(i)).getPath();

		final SplittingBAMIndex idx = new SplittingBAMIndex(
			file.getFileSystem(cfg).open(getIdxPath(file)));

		int splitsEnd = splits.size();
		for (int j = i; j < splitsEnd; ++j)
			if (!file.equals(((FileSplit)splits.get(j)).getPath()))
				splitsEnd = j;

		for (int j = i; j < splitsEnd; ++j) {
			final FileSplit fileSplit = (FileSplit)splits.get(j);

			final long start =         fileSplit.getStart();
			final long end   = start + fileSplit.getLength();

			final Long blockStart = idx.nextAlignment(start);

			// The last split needs to end where the last alignment ends, but the
			// index doesn't store that data (whoops); we only know where the last
			// alignment begins. Fortunately there's no need to change the index
			// format for this: we can just set the end to the maximal length of
			// the final BGZF block (0xffff), and then read until BAMRecordCodec
			// hits EOF.
			final Long blockEnd =
				j == splitsEnd-1 ? idx.prevAlignment(end) | 0xffff
				                 : idx.nextAlignment(end);

			if (blockStart == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block start for " +start);

			if (blockEnd == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block end for " +end);

			newSplits.add(new FileVirtualSplit(
				file, blockStart, blockEnd, fileSplit.getLocations()));
		}
		return splitsEnd;
	}

	// Works the same way as addIndexedSplits, to avoid having to reopen the
	// file repeatedly and checking addIndexedSplits for an index repeatedly.
	private int addProbabilisticSplits(
			List<InputSplit> splits, int i, List<InputSplit> newSplits,
			Configuration cfg)
		throws IOException
	{
		final Path path = ((FileSplit)splits.get(i)).getPath();
		final SeekableStream sin =
			WrapSeekable.openPath(path.getFileSystem(cfg), path);

		final BAMSplitGuesser guesser = new BAMSplitGuesser(sin);

		FileVirtualSplit previousSplit = null;

		for (; i < splits.size(); ++i) {
			FileSplit fspl = (FileSplit)splits.get(i);
			if (!fspl.getPath().equals(path))
				break;

			long beg =       fspl.getStart();
			long end = beg + fspl.getLength();

			long alignedBeg = guesser.guessNextBAMRecordStart(beg, end);

			// As the guesser goes to the next BGZF block before looking for BAM
			// records, the ending BGZF blocks have to always be traversed fully.
			// Hence force the length to be 0xffff, the maximum possible.
			long alignedEnd = end << 16 | 0xffff;

			if (alignedBeg == end) {
				// No records detected in this split: merge it to the previous one.
				// This could legitimately happen e.g. if we have a split that is
				// so small that it only contains the middle part of a BGZF block.
				//
				// Of course, if it's the first split, then this is simply not a
				// valid BAM file.
				//
				// FIXME: In theory, any number of splits could only contain parts
				// of the BAM header before we start to see splits that contain BAM
				// records. For now, we require that the split size is at least as
				// big as the header and don't handle that case.
				if (previousSplit == null)
					throw new IOException("'" + path + "': "+
						"no reads in first split: bad BAM file or tiny split size?");

				previousSplit.setEndVirtualOffset(alignedEnd);
			} else {
				newSplits.add(previousSplit = new FileVirtualSplit(
					path, alignedBeg, alignedEnd, fspl.getLocations()));
			}
		}

		sin.close();
		return i;
	}

	@Override public boolean isSplitable(JobContext job, Path path) {
		return true;
	}
}
