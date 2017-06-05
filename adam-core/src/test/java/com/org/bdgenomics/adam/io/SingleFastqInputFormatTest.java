package com.org.bdgenomics.adam.io;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.bdgenomics.adam.io.SingleFastqInputFormat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SingleFastqInputFormatTest {

  @Test public void readFirstEntry() throws IOException, InterruptedException {
    RecordReader reader = newRecordReader("fastq_sample1.fq");

    // Read the first entry.
    assertTrue(reader.nextKeyValue());
    assertNull(reader.getCurrentKey());
    Text expected = new Text(
        "@H06HDADXX130110:2:2116:3345:91806/1\n"
        + "GTTAGGGTTAGGGTTGGGTTAGGGTTAGGGTTAGGGTTAGGGGTAGGGTTAGGGTTAGGGGTAGGGTTAGGGTTAGGGTTAGGGTTAGGGTTAGGGGTAGGGCTAGGGTTAAGGGTAGGGTTAGCGAAAGGGCTGGGGTTAGGGGTGCGGGTACGCGTAGCATTAGGGCTAGAAGTAGGATCTGCAGTGCCTGACCGCGTCTGCGCGGCGACTGCCCAAAGCCTGGGGCCGACTCCAGGCTGAAGCTCAT\n"
        + "+\n"
        + ">=<=???>?>???=??>>8<?><=2=<===1194<?;:?>>?#3==>###########################################################################################################################################################################################################\n");
    assertEquals(expected, reader.getCurrentValue());
  }

  private RecordReader newRecordReader(String filename) throws IOException, InterruptedException  {
    File testFile = new File("src/test/resources/" + filename);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    Configuration conf = new Configuration(false);
    conf.set("fs.default.name", "file:///");

    InputFormat inputFormat = ReflectionUtils.newInstance(SingleFastqInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    RecordReader reader = inputFormat.createRecordReader(split, context);
    reader.initialize(split, context);
    return reader;
  }
}