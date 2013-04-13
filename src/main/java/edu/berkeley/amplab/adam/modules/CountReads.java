/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.amplab.adam.modules;

import edu.berkeley.amplab.adam.avro.ADAMRecord;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.kohsuke.args4j.Option;

import java.io.IOException;

/**
 * Map-Reduce job to count the total number of reads in the file
 */
public class CountReads extends AdamModule {

  public static class CountReadsMapper extends AvroMapper<ADAMRecord, Pair<CharSequence, Integer>> {

    @Override
    public void map(ADAMRecord record,
                    AvroCollector<Pair<CharSequence, Integer>> collector,
                    Reporter reporter) throws IOException {
      CharSequence refName = record.getReferenceName();
      if (refName == null) {
        refName = "none";
      }
      collector.collect(new Pair<CharSequence, Integer>(refName, 1));
    }
  }

  public static class CountReadsReducer
      extends AvroReducer<CharSequence, Integer, Pair<CharSequence, Integer>> {

    @Override
    public void reduce(CharSequence key, Iterable<Integer> values,
                       AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
        throws IOException {
      int sum = 0;
      for (Integer value : values) {
        sum += value;
      }
      collector.collect(new Pair<CharSequence, Integer>(key, sum));
    }
  }

  @Option(name = "-input", usage = "The ADAM file to read from", required = true)
  private String inputPath;

  @Option(name = "-output", usage = "The directory to write results", required = true)
  private String outputPath;

  @Override
  public String getModuleName() {
    return "count_reads";
  }

  @Override
  public String getModuleDescription() {
    return "Count the number of reads per reference";
  }

  @Override
  public int moduleRun() throws Exception {
    JobConf conf = new JobConf(getConf(), CountReads.class);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroJob.setMapperClass(conf, CountReadsMapper.class);
    AvroJob.setCombinerClass(conf, CountReadsReducer.class);
    AvroJob.setReducerClass(conf, CountReadsReducer.class);

    // Note that AvroJob.setInputSchema and AvroJob.setOutputSchema set
    // relevant config options such as input/output format, map output
    // classes, and output key class.
    AvroJob.setInputSchema(conf, ADAMRecord.SCHEMA$);
    AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Schema.Type.STRING),
                                                     Schema.create(Schema.Type.INT)));
    JobClient.runJob(conf);
    return 0;
  }

}
