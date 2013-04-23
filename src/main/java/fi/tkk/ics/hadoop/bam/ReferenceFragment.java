// Copyright (c) 2012 Aalto University
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

// partly based on SequencedFragment
// note: this class is supposed to represent a single line of a fasta input file, augmented by chromosome/contig name and start position

public class ReferenceFragment implements Writable
{
    protected Text sequence = new Text();
    
    protected Integer position;
    protected String indexSequence;

    public void clear()
    {
	sequence.clear();
	indexSequence = null;
	position = null;
    }

    /**
     * Get sequence Text object.
     * Trade encapsulation for efficiency.  Here we expose the internal Text
     * object so that data may be read and written diretly from/to it.
     *
     * Sequence should always be written using CAPITAL letters and 'N' for unknown bases.
     */
    public Text getSequence() { return sequence; }

    /**
     * Get quality Text object.
     * Trade encapsulation for efficiency.  Here we expose the internal Text
     * object so that data may be read and written diretly from/to it.
     *
     */
    public void setPosition(Integer pos) {
	if (pos == null)
	    throw new IllegalArgumentException("can't have null reference position");
	position = pos;
    }

    public void setIndexSequence(String v) {
	if (v == null)
	    throw new IllegalArgumentException("can't have null index sequence");
	indexSequence = v;
    }

    public void setSequence(Text seq)
    {
	if (seq == null)
	    throw new IllegalArgumentException("can't have a null sequence");
	sequence = seq;
    }

    public Integer getPosition() { return position; }
    public String getIndexSequence() { return indexSequence; }

    /**
     * Recreates a pseudo fasta record with the fields available.
     */
    public String toString()
    {
	String delim = "\t";
	StringBuilder builder = new StringBuilder(800);
	builder.append(indexSequence).append(delim);
	builder.append(position).append(delim);
	builder.append(sequence);
	return builder.toString();
    }

    public boolean equals(Object other)
    {
	if (other != null && other instanceof ReferenceFragment)
	    {
		ReferenceFragment otherFrag = (ReferenceFragment)other;

		if (position == null && otherFrag.position != null || position != null && !position.equals(otherFrag.position))
		    return false;
		if (indexSequence == null && otherFrag.indexSequence != null || indexSequence != null && !indexSequence.equals(otherFrag.indexSequence))
		    return false;
		// sequence can't be null
		if (!sequence.equals(otherFrag.sequence))
		    return false;

		return true;
	    }
	else
	    return false;
    }

    public void readFields(DataInput in) throws IOException
    {
	// serialization order:
	// 1) sequence
	// 2) indexSequence (chromosome/contig name)
	// 3) position of first base in this line of the fasta file

	this.clear();

	sequence.readFields(in);

	indexSequence = WritableUtils.readString(in);
	position = WritableUtils.readVInt(in);
    }

    public void write(DataOutput out) throws IOException
    {
	sequence.write(out);

	WritableUtils.writeString(out, indexSequence);
	WritableUtils.writeVInt(out, position);
	
    }
}
