/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * A record reader for the interleaved FASTQ format.
 *
 * Reads over an input file and parses interleaved FASTQ read pairs into
 * a single Text output. This is then fed into the FastqConverter, which
 * converts the single Text instance into two Alignments.
 */
public abstract class FastqRecordReader extends RecordReader<Void, Text> {
    /*
     * fastq format:
     * <fastq>  :=  <block>+
     * <block>  :=  @<seqname>\n<seq>\n\+[<seqname>]\n<qual>\n
     * <seqname>  :=  [A-Za-z0-9_.:-]+
     * <seq>  :=  [A-Za-z\n\.~]+
     * <qual> :=  [!-~\n]+
     *
     * LP: this format is broken, no?  You can have multi-line sequence and quality strings,
     * and the quality encoding includes '@' in its valid character range.  So how should one
     * distinguish between \n@ as a record delimiter and and \n@ as part of a multi-line
     * quality string?
     *
     * For now I'm going to assume single-line sequences.  This works for our sequencing
     * application.  We'll see if someone complains in other applications.
     */

    /** Default maximum read length, <code>10,000</code> bp. */
    public static final int DEFAULT_MAX_READ_LENGTH = 10000;

    /** Maximum read length property name. */
    public static final String MAX_READ_LENGTH_PROPERTY = "org.bdgenomics.adam.io.FastqRecordReader.MAX_READ_LENGTH";


    /**
     * Set the maximum read length property to <code>maxReadLength</code>.
     *
     * @param conf configuration
     * @param maxReadLength maximum read length, in base pairs (bp)
     */
    public static void setMaxReadLength(final Configuration conf,
					final int maxReadLength) {
	conf.setInt(MAX_READ_LENGTH_PROPERTY, maxReadLength);
    }
	
    /**
     * First valid data index in the stream.
     */
    private long start;

    /**
     * First index value beyond the slice, i.e. slice is in range [start,end).
     */
    protected long end;

    /**
     * Current position in file.
     */
    protected long pos;

    /**
     * Path of the file being parsed.
     */
    private Path file;

    /**
     * The line reader we are using to read the file.
     */
    private LineReader lineReader;

    /**
     * The input stream we are using to read the file.
     */
    private InputStream inputStream;

    /**
     * The text for a single record pair we have parsed out.
     * Hadoop's RecordReader contract requires us to save this as state.
     */
    private Text currentValue;

    /**
     * Newline string for matching on.
     */
    private static final byte[] newline = "\n".getBytes();

    /**
     * Maximum length for a read string.
     */
    private int maxLineLength;

    /**
     * True if the underlying data is splittable.
     */
    protected boolean isSplittable = false;

    /**
     * True if the underlying data is compressed.
     */
    protected boolean isCompressed = false;

    /**
     * True if the last read was &lt; 0 bytes in size.
     */
    private boolean lastReadWasZeroBytes = false;

    /**
     * True if we hit the end of the split in a compressed stream.
     */
    private boolean endOfCompressedSplit = false;
    
    /**
     * Builds a new record reader given a config file and an input split.
     *
     * @param conf The Hadoop configuration object. Used for gaining access
     *   to the underlying file system.
     * @param split The file split to read.
     */
    protected FastqRecordReader(final Configuration conf,
                                final FileSplit split) throws IOException {
	maxLineLength = conf.getInt(MAX_READ_LENGTH_PROPERTY, DEFAULT_MAX_READ_LENGTH);

        file = split.getPath();
        start = split.getStart();
        end = start + split.getLength();

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file);

        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = codecFactory.getCodec(file);

        // if our codec is splittable, we can (tentatively) say that
        // we too are splittable.
        //
        // if we get a bgzfenhancedcodec, the codec might not actually
        // be splittable. however, if we get a non-splittable gz file,
        // several things happen:
        //
        // 1. the input format will detect this, and will not split the
        //    file
        // 2. the bgzfenhancedcodec will check the underlying data type
        //    (BGZF vs GZIP) at input stream creation time, and will
        //    apply the appropriate codec.
        //
        // if we get an unsplittable codec, really all that we do differently
        // is skip the positioning check, since we know that we're at the
        // start of the file and can get to reading immediately
        isSplittable = (codec instanceof SplittableCompressionCodec);
        
        if (codec == null) {
            // no codec.  Uncompressed file.
            int bytesToSkip = positionAtFirstRecord(fileIn, null);
            inputStream = fileIn;
            inputStream.skip(bytesToSkip);
            lineReader = new LineReader(inputStream);
        } else if (isSplittable) {
            // file is compressed, but uses a splittable codec
            isCompressed = true;
            int bytesToSkip = positionAtFirstRecord(fileIn, codec);

            // apparent fun finding: if you don't seek back to 0,
            // SplittableCompressionCodec.createInputStream will seek in the stream
            // to a start position, and funny things happen..
            fileIn.seek(0);
            inputStream = ((SplittableCompressionCodec)codec).createInputStream(fileIn,
                                                                                codec.createDecompressor(),
                                                                                start,
                                                                                end,
                                                                                SplittableCompressionCodec.READ_MODE.BYBLOCK);

            inputStream.skip(bytesToSkip);
            lineReader = new ResettableCompressedSplitLineReader((SplitCompressionInputStream)inputStream, conf);
        } else {
            // unsplittable compressed file
            // expect a single split, first record at offset 0
            isCompressed = true;
            inputStream = codec.createInputStream(fileIn);
            end = Long.MAX_VALUE; // read until the end of the file
            lineReader = new LineReader(inputStream);
        }
    }

    /**
     * Checks to see whether the buffer is positioned at a valid record.
     *
     * @param bufferLength The length of the line currently in the buffer.
     * @param buffer A buffer containing a peek at the first line in the current
     *   stream.
     * @return Returns true if the buffer contains the first line of a properly
     *   formatted FASTQ record.
     */
    abstract protected boolean checkBuffer(int bufferLength, Text buffer);

    /**
     * Position the input stream at the start of the first record.
     *
     * @param stream The stream to reposition.
     */
    protected final int positionAtFirstRecord(final FSDataInputStream stream,
                                              final CompressionCodec codec) throws IOException {
        Text buffer = new Text();
        long originalStart = start;
        
        LineReader reader;
        if (codec == null) {
            // Advance to the start of the first record that ends with /1
            // We use a temporary LineReader to read lines until we find the
            // position of the right one.  We then seek the file to that position.
            stream.seek(start);
            reader = new LineReader(stream);
        } else {
            // Unlike the codec == null case, we don't seek before creating the
            // reader, SplittableCompressionCodec.createInputStream places the
            // stream at the start of the first compression block after our
            // split start
            //
            // as noted above, we need to be at pos 0 in the stream before
            // calling this
            reader = new LineReader(((SplittableCompressionCodec)codec).createInputStream(stream,
                                                                                          null,
                                                                                          start,
                                                                                          end,
                                                                                          SplittableCompressionCodec.READ_MODE.BYBLOCK));
        }
        
        int bytesRead = 0;
        do {
            bytesRead = reader.readLine(buffer, (int) Math.min(maxLineLength, end - start));
            int bufferLength = buffer.getLength();
            if (bytesRead > 0 && !checkBuffer(bufferLength, buffer)) {
                start += bytesRead;
            } else {

                // line starts with @.  Read two more and verify that it starts
                // with a +:
                //
                // @<readname>
                // <sequence>
                // +[readname]
                //
                // if the second line we read starts with a @, we know that
                // we've read:
                //
                // <qualities> <-- @ is a valid ASCII phred encoding
                // @<readname>
                //
                // and thus, the second read is the delimiter and we can break
                long trackForwardPosition = start + bytesRead;
                
                bytesRead = reader.readLine(buffer, (int) Math.min(maxLineLength, end - start));
                if (buffer.getLength() > 0 && buffer.getBytes()[0] == '@') {
                    start = trackForwardPosition;
                    break;
                } else {
                    trackForwardPosition += bytesRead;
                }

                bytesRead = reader.readLine(buffer, (int) Math.min(maxLineLength, end - start));
                trackForwardPosition += bytesRead;
                if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+') {
                    break; // all good!
                } else {
                    start = trackForwardPosition;
                }
            }
        } while (bytesRead > 0);

        pos = start;
        start = originalStart;
        stream.seek(start);
        return (int)(pos - originalStart);
    }

    public final void initialize(final InputSplit split, final TaskAttemptContext context)
        throws IOException, InterruptedException {
        // this method does nothing but is required by
        // org.apache.hadoop.mapreduce.RecordReader
    }
    
    /**
     * FASTQ has no keys, so we return null.
     *
     * @return Always returns null.
     */
    public final Void getCurrentKey() {
        return null;
    }

    /**
     * Returns the last interleaved FASTQ record.
     *
     * @return The text corresponding to the last read pair.
     */
    public final Text getCurrentValue() {
        return currentValue;
    }

    /**
     * Seeks ahead in our split to the next key-value pair.
     *
     * Triggers the read of an interleaved FASTQ read pair, and populates
     * internal state.
     *
     * @return True if reading the next read pair succeeded.
     */
    public final boolean nextKeyValue() throws IOException, InterruptedException {
        currentValue = new Text();
        return next(currentValue);
    }

    /**
     * Close this RecordReader to future operations.
     */
    public final void close() throws IOException {
        inputStream.close();
    }

    /**
     * How much of the input has the RecordReader consumed?
     *
     * @return Returns a value on [0.0, 1.0] that notes how many bytes we
     *   have read so far out of the total bytes to read.
     */
    public final float getProgress() {
        if (start == end) {
            return 1.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    /**
     * Produces a debugging message with the file position.
     *
     * @return Returns a string containing {filename}:{index}.
     */
    protected final String makePositionMessage() {
        return file.toString() + ":" + pos;
    }

    /**
     * Parses a read from an interleaved FASTQ file.
     *
     * Only reads a single record.
     *
     * @param readName Text record containing read name. Output parameter.
     * @param value Text record containing full record. Output parameter.
     * @return Returns true if read was successful (did not hit EOF).
     *
     * @throws RuntimeException Throws exception if FASTQ record doesn't
     *   have proper formatting (e.g., record doesn't start with @).
     */
    protected final boolean lowLevelFastqRead(final Text readName, final Text value)
            throws IOException {

        if (endOfCompressedSplit) {
            return false;
        }

        // ID line
        readName.clear();
        long skipped = appendLineInto(readName, true);
        if (skipped == 0) {
            return false; // EOF
        }

        if (readName.getBytes()[0] != '@') {
            throw new RuntimeException("unexpected fastq record didn't start with '@' at " +
                                       makePositionMessage() +
                                       ". Line: " +
                                       readName + ". \n");
        }
        value.append(readName.getBytes(), 0, readName.getLength());

        // sequence
        appendLineInto(value, false);

        // separator line
        appendLineInto(value, false);

        // quality
        appendLineInto(value, false);

        return true;
    }

    /**
     * Reads from the input split.
     *
     * @param value Text record to write input value into.
     * @return Returns whether this read was successful or not.
     *
     * @see #lowLevelFastqRead(Text, Text)
     */
    abstract protected boolean next(Text value) throws IOException;

    /**
     * Reads a newline into a text record from the underlying line reader.
     *
     * @param dest Text record to read line into.
     * @param eofOk Whether an EOF is acceptable in this line.
     * @return Returns the number of bytes read.
     *
     * @throws EOFException Throws if eofOk was false and we hit an EOF in
     *    the current line.
     */
    private int appendLineInto(final Text dest, final boolean eofOk) throws EOFException, IOException {
        Text buf = new Text();
        int bytesRead = lineReader.readLine(buf, (int) Math.min(maxLineLength, end - start));

        // ok, so first, split/unsplit, compressed/uncompressed notwithstanding,
        // there are three cases we can run into:
        //
        // 1. we read data
        // 2. we are at an acceptable eof/end-of-split and don't read data
        // 3. we are at an unacceptable eof/end-of-split and don't read data
        //
        // cases 1 and 2 are consistent across split/unsplit, compressed/uncompressed.
        //
        // case 3 is simple in the unsplit or uncompressed cases; something has
        // gone wrong, we throw an EOFException, and move on with our lives
        //
        // case 3 is where working with split compressed files gets fun.
        //
        // with the split compression stream, the first time we read past the
        // end of the last compression block within a file split, we get no
        // bytes back. the BZip2Codec and BGZFCodec's actually tell us that
        // we'll get -2 back in this case, but we'll cast a wider net yet.
        //
        // this is important information---if we don't know this, we'll keep reading
        // past the end of the split to the end of the file---but we still need to
        // finish reading our multiline record, so we set some state to let us know
        // that we're reading the last record in the split (endOfCompressedSplit)
        // and repeat the read. if the read fails again, then that means that
        // something has actually gone wrong, and we want to fall through and
        // throw an EOFException or return no bytes read (depending on eofOk).
        // that's why we have the lastReadWasZeroBytes flag around. we set this
        // to true on the first read that gets bytesRead <= 0, and clear it on
        // any read that reads more than 0 bytes.
        if (isSplittable &&
            isCompressed &&
            !lastReadWasZeroBytes &&
            bytesRead <= 0 &&
            !eofOk) {

            // we need to clear the reader state so we can continue reading
            ((ResettableCompressedSplitLineReader)lineReader).reset();

            // set the state to stop us from reading another record and
            // to catch back-to-back failed reads
            lastReadWasZeroBytes = true;
            endOfCompressedSplit = true;

            // recursively call to redo the read
            return appendLineInto(dest, eofOk);
        } else if (bytesRead < 0 || (bytesRead == 0 && !eofOk)) {
            throw new EOFException();
        } else {
            lastReadWasZeroBytes = false;
        }

        dest.append(buf.getBytes(), 0, buf.getLength());
        dest.append(newline, 0, 1);
        if (isSplittable && isCompressed) {
            pos = ((SplitCompressionInputStream)inputStream).getPos();
        } else {
            pos += bytesRead;
        }

        return bytesRead;
    }
}
