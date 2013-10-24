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

package fi.tkk.ics.hadoop.bam.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import parquet.hadoop.util.ContextUtil;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for BGZF-compressed
 * files.
 *
 * <p>A {@link BGZFBlockIndex} for each Path used is required, or an
 * <code>IOException</code> is thrown out of {@link #getSplits}.</p>
 */
public abstract class BGZFSplitFileInputFormat<K,V>
	extends FileInputFormat<K,V>
{
	private Path getIdxPath(Path path) { return path.suffix(".bgzfi"); }

	/** The splits returned are FileSplits. */
	@Override public List<InputSplit> getSplits(JobContext job)
		throws IOException
	{
		final List<InputSplit> splits = super.getSplits(job);

		// Align the splits so that they don't cross blocks

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

		final Configuration cfg = ContextUtil.getConfiguration(job);

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

		final BGZFBlockIndex idx = new BGZFBlockIndex(
			file.getFileSystem(cfg).open(getIdxPath(file)));

		int splitsEnd = splits.size();
		for (int j = i; j < splitsEnd; ++j)
			if (!file.equals(((FileSplit)splits.get(j)).getPath()))
				splitsEnd = j;

		for (int j = i; j < splitsEnd; ++j) {
			final FileSplit fileSplit = (FileSplit)splits.get(j);

			final long start =         fileSplit.getStart();
			final long end   = start + fileSplit.getLength();

			final Long blockStart = idx.prevBlock(start);
			final Long blockEnd   = j == splitsEnd-1 ? idx.prevBlock(end)
			                                         : idx.nextBlock(end);

			if (blockStart == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block start for " +start);

			if (blockEnd == null)
				throw new RuntimeException(
					"Internal error or invalid index: no block end for " +end);

			newSplits.add(new FileSplit(
				file, blockStart, blockEnd - blockStart,
				fileSplit.getLocations()));
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
		final FSDataInputStream in = path.getFileSystem(cfg).open(path);

		final BGZFSplitGuesser guesser = new BGZFSplitGuesser(in);

		FileSplit fspl;
		do {
			fspl = (FileSplit)splits.get(i);

			final long beg =       fspl.getStart();
			final long end = beg + fspl.getLength();

			final long alignedBeg = guesser.guessNextBGZFBlockStart(beg, end);

			newSplits.add(new FileSplit(
				path, alignedBeg, end - alignedBeg, fspl.getLocations()));

			++i;
		} while (i < splits.size() && fspl.getPath().equals(path));

		in.close();
		return i;
	}

	@Override public boolean isSplitable(JobContext job, Path path) {
		return true;
	}
}
