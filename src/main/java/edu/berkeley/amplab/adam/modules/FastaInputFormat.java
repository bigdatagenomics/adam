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

import com.google.common.base.CharMatcher;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import edu.berkeley.amplab.adam.avro.ADAMFastaFragment;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class FastaInputFormat extends FileInputFormat<Void, AvroValue<ADAMFastaFragment>> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Void, AvroValue<ADAMFastaFragment>> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new FastaRecordReader();
    }

    public class FastaRecordReader extends RecordReader<Void, AvroValue<ADAMFastaFragment>> {

        private LineRecordReader lineRecordReader = new LineRecordReader();
        private String description;
        private Long position;
        private AvroValue<ADAMFastaFragment> value = new AvroValue<ADAMFastaFragment>();
        final CharMatcher validCharacters = CharMatcher.inRange('a', 'z').or(CharMatcher.inRange('A', 'Z'));
        private Predicate<String> isDescriptionLine = new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(">");
            }
        };
        private Predicate<String> isCommentLine = new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(";");
            }
        };
        // The first description line can be represented as a comment line...
        private Predicate<String> descriptionPredicate = Predicates.or(isDescriptionLine, isCommentLine);

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            lineRecordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (lineRecordReader.nextKeyValue()) {
                String line = lineRecordReader.getCurrentValue().toString();
                if (descriptionPredicate.apply(line)) {
                    description = line.substring(1);
                    position = 0L;
                } else if (!isCommentLine.apply(line)) {
                    // Watch out for lines that e.g. can end with '*'
                    String cleanLine = validCharacters.retainFrom(line);
                    if (cleanLine.length() > 0) {
                        long start = position;
                        long end = start + cleanLine.length() - 1;
                        position += cleanLine.length();
                        ADAMFastaFragment fragment = ADAMFastaFragment.newBuilder()
                                .setDescription(description)
                                .setStart(start)
                                .setEnd(end)
                                .setSequence(cleanLine)
                                .build();
                        value.datum(fragment);
                    }
                    return true;
                }
            }
            // Now that we found the first description, use the standard description predicate
            descriptionPredicate = isDescriptionLine;
            return false;
        }

        @Override
        public Void getCurrentKey() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public AvroValue<ADAMFastaFragment> getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    }
}
