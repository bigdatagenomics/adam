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

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord;
import edu.berkeley.cs.amplab.adam.converters.BAMRecordConverter;
import edu.berkeley.cs.amplab.adam.converters.SpecificRecordConverter;
import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.Argument;

import java.io.IOException;

public class ConvertBAM extends ImportSupport {

    @Override
    public String getModuleName() {
        return "convert_bam";
    }

    @Override
    public String getModuleDescription() {
        return "Converts a SAM/BAM file into a new ADAM read datafile";
    }

    public static class RecordMapper extends Mapper<LongWritable, SAMRecordWritable,
            AvroKey<Long>, AvroValue<ADAMRecord>> {

        private AvroKey<Long> key = new AvroKey<Long>();
        private AvroValue<ADAMRecord> value = new AvroValue<ADAMRecord>();
        private BAMRecordConverter converter = new BAMRecordConverter();

        @Override
        protected void map(LongWritable ignore, SAMRecordWritable record, Context context)
                throws IOException, InterruptedException {
            key.datum((long) record.get().getAlignmentStart());
            value.datum(converter.apply(record.get()));
            context.write(key, value);
        }
    }

    public static class RecordReducer extends Reducer<AvroKey<Long>, AvroValue<ADAMRecord>,
            Void, GenericRecord> {

        private final SpecificRecordConverter<ADAMRecord> converter =
                new SpecificRecordConverter<ADAMRecord>();

        @Override
        protected void reduce(AvroKey<Long> positions, Iterable<AvroValue<ADAMRecord>> values,
                              Context context) throws IOException, InterruptedException {
            for (AvroValue<ADAMRecord> value : values) {
                context.write(null, converter.apply(value.datum()));
            }
        }
    }

    @Argument(required = true, metaVar = "FILE", usage = "The SAM or ADAM file to convert")
    public String inputFile;

    @Override
    public int moduleRun() throws Exception {
        final Job job = setupJob();

        BAMInputFormat.addInputPath(job, new Path(inputFile));
        job.setInputFormatClass(BAMInputFormat.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.LONG));
        AvroJob.setMapOutputValueSchema(job, ADAMRecord.SCHEMA$);

        job.setMapperClass(RecordMapper.class);
        job.setReducerClass(RecordReducer.class);

        setupOutputFormat(job, ADAMRecord.SCHEMA$, generateOutputPath(inputFile));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ConvertBAM(), args));
    }

}
