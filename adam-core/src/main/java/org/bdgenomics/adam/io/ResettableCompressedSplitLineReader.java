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

import java.io.InputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

/**
 * A line reader that can continue beyond the end of a split after being reset.
 *
 * This class cribs heavily from
 * org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader. While the
 * CompressedSplitLineReader class does 99% of what we wanted, it has one
 * major limitation: specifically, it can only read one line past the end of
 * a compressed block that overlaps a split boundary. While this is sufficient
 * for file formats where each line is a single record (e.g., VCF, GFF, etc),
 * FASTQ consumes 4 lines per record and interleaved FASTQ is a whopping 8
 * lines per record!
 *
 * To support multiple-line records, we add the ability to "reset" the reader
 * at the end of a block. When the reader hits the last line in a split, we
 * will initially read &lt; 0 bytes. This is the sign to the caller that we
 * are in the record that is at the end of the split. After this, the caller
 * should reset this LineReader. This clears the internal state that prevents
 * us from reading beyond the end of a split and allows the caller to read as
 * many more lines as they need to finish reading their record.
 */
class ResettableCompressedSplitLineReader extends SplitLineReader {

    /**
     * Have we hit the end of the split we are reading from?
     *
     * If true, we should not read from the stream.
     */ 
    private boolean isFinished = false;

    /**
     * Do we need to read another line past the end of the split?
     *
     * This is true if we have read past the end but do not have a newline.
     */
    private boolean needsAdditionalRecord = false;

    /**
     * The stream we are reading from.
     */ 
    private final SplitCompressionInputStream sin;

    /**
     * @param in The stream to read from.
     * @param conf The hadoop configuration for the job using this line reader.
     */
    public ResettableCompressedSplitLineReader(final SplitCompressionInputStream in,
                                               final Configuration conf)
        throws IOException {
        super(in, conf, null);
        sin = in;
    }

    /**
     * Resets the internal state that prevents reading past the end of a split.
     *
     * Should be called at the end of a split if reading a multi-line record and
     * there are lines remaining in the record at the end of the split.
     */
    public void reset() {
        isFinished = false;
        needsAdditionalRecord = true;
    }
    
    @Override
    public int readLine(final Text str,
                        final int maxLineLength,
                        final int maxBytesToConsume)
        throws IOException {
        int bytesRead = 0;
        if (!isFinished) {
            // only allow at most one more record to be read after the stream
            // reports the split ended
            if (sin.getPos() > sin.getAdjustedEnd()) {
                isFinished = true;
            }
            
            bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
        }
        return bytesRead;
    }
    
    @Override
    public boolean needAdditionalRecordAfterSplit() {
        return !isFinished && needsAdditionalRecord;
    }

    @Override
    protected int fillBuffer(final InputStream in,
                             final byte[] buffer,
                             final boolean inDelimiter)
        throws IOException {
        int bytesRead = in.read(buffer);
        
        // If the split ended in the middle of a record delimiter then we need
        // to read one additional record, as the consumer of the next split will
        // not recognize the partial delimiter as a record.
        // However if using the default delimiter and the next character is a
        // linefeed then next split will treat it as a delimiter all by itself
        // and the additional record read should not be performed.
        if (inDelimiter && bytesRead > 0) {
            needsAdditionalRecord = (buffer[0] != '\n');
        }
        
        return bytesRead;          
    }
}
