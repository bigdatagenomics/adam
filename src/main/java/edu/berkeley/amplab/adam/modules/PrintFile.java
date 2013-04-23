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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import edu.berkeley.amplab.adam.avro.ADAMFastaFragment;
import edu.berkeley.amplab.adam.avro.ADAMRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.Option;
import parquet.avro.AvroParquetInputFormat;
import parquet.hadoop.util.ContextUtil;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class PrintFile extends AdamModule {
    public static String COLUMNS_INCLUDE = "adam.columns.include";
    public static String COLUMNS_EXCLUDE = "adam.columns.exclude";
    public static String COLUMN_DELIMITER = "adam.columns.delimiter";
    static Joiner COMMA_JOINER = Joiner.on(",").skipNulls();
    static Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

    @Override
    public String getModuleName() {
        return "print";
    }

    @Override
    public String getModuleDescription() {
        return "Print an ADAM file to a file";
    }

    public static class PrintFileMapper extends Mapper<Void, GenericRecord, LongWritable, Text> {

        private Set<String> exclude;
        private Set<String> include;
        private Joiner columnJoiner;
        private String sortKeyName;

        private Set<String> createColumnSet(String config) {
            if (config == null) {
                return Sets.newHashSet();
            } else {
                return Sets.newHashSet(COMMA_SPLITTER.split(config));
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            exclude = createColumnSet(config.get(COLUMNS_EXCLUDE));
            include = createColumnSet(config.get(COLUMNS_INCLUDE));
            columnJoiner = Joiner.on(config.get(COLUMN_DELIMITER));
        }

        private List<String> getFieldNames(GenericRecord record) {
            return Lists.transform(record.getSchema().getFields(),
                    new Function<Schema.Field, String>() {
                        @Override
                        public String apply(Schema.Field field) {
                            return field.name();
                        }
                    });
        }

        private String getSortKey(GenericRecord record) {
            if (sortKeyName != null) {
                return sortKeyName;
            }
            if (record.getSchema().equals(ADAMRecord.SCHEMA$)) {
                sortKeyName = "start";
            } else if (record.getSchema().equals(ADAMFastaFragment.SCHEMA$)) {
                sortKeyName = "start";
            } else {
                throw new IllegalArgumentException("Unable to determine sort key name");
            }
            return sortKeyName;
        }

        @Override
        protected void map(Void key, GenericRecord record, Context context) throws IOException, InterruptedException {
            List<String> fieldNames = getFieldNames(record);
            Set<String> columnsToMaterialize = Sets.newHashSet(fieldNames);
            if (include.size() > 0) {
                for (String includeColumnName : include) {
                    Preconditions.checkState(fieldNames.contains(includeColumnName),
                            String.format("There is no column named '%s'", includeColumnName));
                }
                columnsToMaterialize = include;
            }
            if (exclude.size() > 0) {
                columnsToMaterialize.removeAll(exclude);
            }
            List<String> columnValues = Lists.newArrayList();
            if (columnsToMaterialize.size() > 0) {
                for (String columnName : fieldNames) {
                    if (columnsToMaterialize.contains(columnName)) {
                        Object value = record.get(columnName);
                        if (value == null) {
                            value = "null";
                        }
                        columnValues.add(value.toString());
                    }
                }
            }
            String sortKeyName = getSortKey(record);
            Long sortKey = (Long) record.get(sortKeyName);
            context.write(new LongWritable(sortKey),
                    new Text(columnJoiner.join(columnValues)));
        }
    }

    public static class PrintFileReducer extends Reducer<LongWritable, Text,
            Text, Void> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, null);
            }
        }
    }

    @Option(name = "-input", required = true, usage = "Path to ADAM data")
    String inputPath;

    @Option(name = "-output", required = true, usage = "Output File", metaVar = "FILE")
    String outputPath;

    @Option(name = "-include", usage = "Columns to include in output. Default is all.",
            metaVar = "COLUMN_NAME")
    List<String> includeColumns = Lists.newArrayList();

    @Option(name = "-exclude", usage = "Columns to exclude in output. Default is none.",
            metaVar = "COLUMN_NAME")
    List<String> excludeColumns = Lists.newArrayList();

    @Option(name = "-column_delimiter", usage = "The record delimiter. Default is ','",
            metaVar = "DELIMITER")
    String columnDelimiter = ",";

    private void setColumnConfiguration(Configuration config, String name, List<String> values) {
        if (values.size() > 0) {
            config.set(name, COMMA_JOINER.join(values));
        }
    }

    @Override
    public int moduleRun() throws Exception {
        final Job job = new Job(getConf(), getModuleName());
        Configuration config = ContextUtil.getConfiguration(job);
        setColumnConfiguration(config, COLUMNS_INCLUDE, includeColumns);
        setColumnConfiguration(config, COLUMNS_EXCLUDE, excludeColumns);
        config.set(COLUMN_DELIMITER, columnDelimiter);
        job.setJarByClass(PrintFile.class);
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.setInputPaths(job, new Path(inputPath));

        job.setMapperClass(PrintFileMapper.class);
        job.setReducerClass(PrintFileReducer.class);

        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // For debugging...
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PrintFile(), args));
    }


}

