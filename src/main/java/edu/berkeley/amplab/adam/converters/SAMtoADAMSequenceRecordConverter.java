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

package edu.berkeley.amplab.adam.converters;

import edu.berkeley.amplab.adam.avro.ADAMSequenceRecord;
import edu.berkeley.amplab.adam.avro.ADAMSequenceRecordAttribute;
import net.sf.samtools.SAMSequenceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts SAMSequenceRecord to ADAMSequenceRecord
 */
public class SAMtoADAMSequenceRecordConverter
    implements Converter<SAMSequenceRecord, ADAMSequenceRecord> {

  @Override
  public ADAMSequenceRecord convert(SAMSequenceRecord samSequenceRecord) {

    List<ADAMSequenceRecordAttribute> attrs =
        new ArrayList<ADAMSequenceRecordAttribute>();
    for (Map.Entry<String, String> entry : samSequenceRecord.getAttributes()) {
      attrs.add(
          new ADAMSequenceRecordAttribute(entry.getKey(), entry.getValue()));
    }

    // TODO md5 and Uri
    return ADAMSequenceRecord.newBuilder()
                             .setAssemblyId(samSequenceRecord.getAssembly())
                             .setReferenceId(samSequenceRecord.getSequenceIndex())
                             .setReferenceLength(samSequenceRecord.getSequenceLength())
                             .setReferenceName(samSequenceRecord.getSequenceName())
                             .setSpecies(samSequenceRecord.getSpecies())
                             .setMd5(null)
                             .setUri(null)
                             .setAttributes(attrs)
                             .build();
  }
}
