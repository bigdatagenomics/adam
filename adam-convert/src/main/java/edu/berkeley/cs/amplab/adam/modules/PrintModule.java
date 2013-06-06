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

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.Argument;
import parquet.avro.AvroParquetReader;

import java.util.ArrayList;
import java.util.List;

public class PrintModule extends AdamModule {

    @Override
    public String getModuleName() {
        return "print";
    }

    @Override
    public String getModuleDescription() {
        return "Print an ADAM file to console";
    }

    @Argument(required = true, metaVar = "FILE(S)", multiValued = true, usage = "One or more files to print")
    private List<String> filesToPrint = new ArrayList<String>();

    @Override
    public int moduleRun() throws Exception {

        FileSystem fs = FileSystem.get(getConf());
        for (String fileToPrint : filesToPrint) {
            Path inputPath = new Path(fileToPrint);
            if (!fs.exists(inputPath)) {
                System.err.println(String.format("The path '%s' does not exist", fileToPrint));
                System.exit(1);
            }
            List<Path> paths = Lists.newArrayList();
            if (fs.isDirectory(inputPath)) {
                FileStatus[] files = fs.listStatus(inputPath);
                for (FileStatus file : files) {
                    if (file.getPath().getName().startsWith("part-")) {
                        paths.add(file.getPath());
                    }
                }
            } else if (fs.isFile(inputPath)) {
                paths.add(inputPath);
            } else {
                System.err.println(String.format("The path '%s' is neither a file or directory", fileToPrint));
                System.exit(1);
            }
            for (Path path : paths) {
                AvroParquetReader reader = new AvroParquetReader(path);
                for (; ; ) {
                    GenericRecord record = (GenericRecord) reader.read();
                    if (record == null) {
                        break;
                    }
                    System.out.println(record);
                }
                reader.close();
            }
        }
        return 0;
    }

    // For debugging...
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PrintModule(), args));
    }

}

