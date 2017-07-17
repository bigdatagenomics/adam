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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class FastqInputFormat extends FileInputFormat<Void, Text> {

    /**
     * For now, we do not support splittable compression codecs. As in, we will
     * read the compressed data, but we will not allow it to be splittable. We
     * will fix this in #1457.
     *
     * @param context The job context to get the configuration from.
     * @param filename The path the input file is saved at.
     * @return Returns false if this file is compressed.
     */
    @Override protected boolean isSplitable(JobContext context, Path filename) {
        Configuration conf = context.getConfiguration();
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        return (codec == null);
    }
}
