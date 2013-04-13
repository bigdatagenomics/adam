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
import edu.berkeley.amplab.adam.converters.SAMtoADAMRecordConverter;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ConvertFile extends AdamModule {

  @Option(name = "-input", required = true, usage = "SAM/BAM input file path")
  private String inputPath;

  @Option(name = "-output", required = true, usage = "SAM/BAM output file path")
  private String outputPath;

  @Option(name = "-read_from_hadoop",
          usage = "Read input from HDFS instead of locally (default=false)")
  private boolean readFromHadoop = false;

  @Option(name = "-write_to_local", usage = "Write output to the local filesystem (default=false)")
  private boolean writeToLocal = false;

  @Option(name = "-avro_sync_interval", usage = "Avro sync interval (default=64k)")
  private int syncInterval = 64 * 1024;

  @Option(name = "-compressionLevel", usage = "Compression level for deflate (default=2)")
  private int compressionLevel = 2;

  @Override
  public String getModuleName() {
    return "convert";
  }

  @Override
  public String getModuleDescription() {
    return "Convert a SAM/BAM file to the ADAM format";
  }

  @Override
  public int moduleRun() throws Exception {
    // Setup the SAM reader
    SAMFileReader samReader;
    if (readFromHadoop) {
      FileSystem fs = FileSystem.get(getConf());
      FSDataInputStream inputStream = fs.open(new Path(inputPath));
      samReader = new SAMFileReader(inputStream);
    } else {
      samReader = new SAMFileReader(new File(inputPath));
    }
    samReader.setValidationStringency(SAMFileReader.ValidationStringency.LENIENT);

    // Setup the DataFileWriter
    String samHeaderMetaName = "sam.header";
    DataFileWriter<ADAMRecord> avroRecordsFile;
    if (writeToLocal) {
      File outputFile = new File(outputPath);
      avroRecordsFile = new DataFileWriter<ADAMRecord>(
          new GenericDatumWriter<ADAMRecord>(ADAMRecord.SCHEMA$))
          .setCodec(CodecFactory.deflateCodec(compressionLevel))
          .setSyncInterval(syncInterval)
          .setMeta(samHeaderMetaName, samReader.getFileHeader().getTextHeader())
          .create(ADAMRecord.SCHEMA$, outputFile);
    } else {
      FileSystem fs = FileSystem.get(getConf());
      FSDataOutputStream out = fs.create(new Path(outputPath));
      avroRecordsFile = new DataFileWriter<ADAMRecord>(
          new GenericDatumWriter<ADAMRecord>(ADAMRecord.SCHEMA$))
          .setCodec(CodecFactory.deflateCodec(compressionLevel))
          .setSyncInterval(syncInterval)
          .setMeta(samHeaderMetaName, samReader.getFileHeader().getTextHeader())
          .create(ADAMRecord.SCHEMA$, out);
    }

    long start = System.currentTimeMillis();
    SAMtoADAMRecordConverter recordConverter = new SAMtoADAMRecordConverter();
    long i = 0;
    for (SAMRecord record : samReader) {
      i++;
      ADAMRecord adamRecord = recordConverter.convert(record);
      avroRecordsFile.append(adamRecord);
      if (i % 10000 == 0) {
        System.out.print(
            String.format("reference %s @%10d                                          \r",
                          adamRecord.getReferenceName(), adamRecord.getStart()));
      }
    }
    System.out.println("\n" +
                           TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) +
                           " secs to convert " + i + " reads");
    samReader.close();
    avroRecordsFile.close();
    return 0;
  }

}
