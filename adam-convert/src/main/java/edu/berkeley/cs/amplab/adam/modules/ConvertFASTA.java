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

package edu.berkeley.cs.amplab.adam.modules;

import edu.berkeley.cs.amplab.adam.avro.ADAMFastaFragment;
import edu.berkeley.cs.amplab.adam.converters.SpecificRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.Argument;

import java.io.IOException;

public class ConvertFASTA extends ImportSupport {

    @Override
    public String getModuleName() {
        return "convert_fasta";
    }

    @Override
    public String getModuleDescription() {
        return "Converts a FASTA file into a new ADAM reference datafile";
    }

    public static class RecordMapper extends Mapper<Void, AvroValue<ADAMFastaFragment>,
            AvroKey<ADAMFastaFragment>, NullWritable> {

        private AvroKey<ADAMFastaFragment> key = new AvroKey<ADAMFastaFragment>();

        @Override
        protected void map(Void ignore, AvroValue<ADAMFastaFragment> fragment, Context context)
                throws IOException, InterruptedException {
            key.datum(fragment.datum());
            context.write(key, NullWritable.get());
        }
    }

    public static class RecordReducer extends Reducer<AvroKey<ADAMFastaFragment>, NullWritable,
            Void, GenericRecord> {

        private SpecificRecordConverter<ADAMFastaFragment> converter
                = new SpecificRecordConverter<ADAMFastaFragment>();

        @Override
        protected void reduce(AvroKey<ADAMFastaFragment> key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(null, converter.apply(key.datum()));
        }
    }

    @Argument(required = true, metaVar = "FILE", usage = "The FASTA file to import into a new ADAM datastore")
    public String inputFile;

    @Override
    public int moduleRun() throws Exception {
        final Job job = setupJob();

        FastaInputFormat.addInputPath(job, new Path(inputFile));
        job.setInputFormatClass(FastaInputFormat.class);
        AvroJob.setMapOutputKeySchema(job, ADAMFastaFragment.SCHEMA$);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(RecordMapper.class);
        job.setReducerClass(RecordReducer.class);

        setupOutputFormat(job, ADAMFastaFragment.SCHEMA$, generateOutputPath(inputFile));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ConvertFASTA(), args));
    }

}
