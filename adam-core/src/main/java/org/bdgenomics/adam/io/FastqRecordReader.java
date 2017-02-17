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
 * converts the single Text instance into two AlignmentRecords.
 */
abstract class FastqRecordReader extends RecordReader<Void, Text> {
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
    private static final int MAX_LINE_LENGTH = 10000;

    /**
     * Builds a new record reader given a config file and an input split.
     *
     * @param conf The Hadoop configuration object. Used for gaining access
     *   to the underlying file system.
     * @param split The file split to read.
     */
    protected FastqRecordReader(final Configuration conf, final FileSplit split) throws IOException {
        file = split.getPath();
        start = split.getStart();
        end = start + split.getLength();

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file);

        CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = codecFactory.getCodec(file);

        if (codec == null) { // no codec.  Uncompressed file.
            positionAtFirstRecord(fileIn);
            inputStream = fileIn;
        } else { 
            // compressed file
            if (start != 0) {
                throw new RuntimeException("Start position for compressed file is not 0! (found " + start + ")");
            }
            inputStream = codec.createInputStream(fileIn);
            end = Long.MAX_VALUE; // read until the end of the file
        }
        lineReader = new LineReader(inputStream);
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
    protected final void positionAtFirstRecord(final FSDataInputStream stream) throws IOException {
        Text buffer = new Text();

        if (true) { // (start > 0) // use start>0 to assume that files start with valid data
            // Advance to the start of the first record that ends with /1
            // We use a temporary LineReader to read lines until we find the
            // position of the right one.  We then seek the file to that position.
            stream.seek(start);
            LineReader reader = new LineReader(stream);

            int bytesRead = 0;
            do {
                bytesRead = reader.readLine(buffer, (int) Math.min(MAX_LINE_LENGTH, end - start));
                int bufferLength = buffer.getLength();
                if (bytesRead > 0 && !checkBuffer(bufferLength, buffer)) {
                    start += bytesRead;
                } else {
                    // line starts with @.  Read two more and verify that it starts with a +
                    //
                    // If this isn't the start of a record, we want to backtrack to its end
                    long backtrackPosition = start + bytesRead;

                    bytesRead = reader.readLine(buffer, (int) Math.min(MAX_LINE_LENGTH, end - start));
                    bytesRead = reader.readLine(buffer, (int) Math.min(MAX_LINE_LENGTH, end - start));
                    if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+') {
                        break; // all good!
                    } else {
                        // backtrack to the end of the record we thought was the start.
                        start = backtrackPosition;
                        stream.seek(start);
                        reader = new LineReader(stream);
                    }
                }
            } while (bytesRead > 0);

            stream.seek(start);
        }
        pos = start;
    }

    /**
     * Method is a no-op.
     *
     * @param split The input split that we will parse.
     * @param context The Hadoop task context.
     */
    public final void initialize(final InputSplit split, final TaskAttemptContext context)
            throws IOException, InterruptedException {}

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
        int bytesRead = lineReader.readLine(buf, (int) Math.min(MAX_LINE_LENGTH, end - start));

        if (bytesRead < 0 || (bytesRead == 0 && !eofOk)) {
            throw new EOFException();
        }

        dest.append(buf.getBytes(), 0, buf.getLength());
        dest.append(newline, 0, 1);
        pos += bytesRead;

        return bytesRead;
    }
}