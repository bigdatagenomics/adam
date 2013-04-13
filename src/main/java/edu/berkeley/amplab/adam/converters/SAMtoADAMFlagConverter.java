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

import edu.berkeley.amplab.adam.avro.ADAMAlignmentFlag;
import net.sf.samtools.SAMRecord;

import java.util.ArrayList;
import java.util.List;

public class SAMtoADAMFlagConverter implements Converter<SAMRecord, List<ADAMAlignmentFlag>> {

  @Override
  public List<ADAMAlignmentFlag> convert(SAMRecord samRecord) {
    if (samRecord.getFlags() == 0) {
      return null;
    }
    List<ADAMAlignmentFlag> flags = new ArrayList<ADAMAlignmentFlag>();
    if (samRecord.getDuplicateReadFlag()) {
      flags.add(ADAMAlignmentFlag.DUPLICATE_READ);
    }
    if (samRecord.getReadNegativeStrandFlag()) {
      flags.add(ADAMAlignmentFlag.READ_NEGATIVE_STRAND);
    }
    if (samRecord.getMateNegativeStrandFlag()) {
      flags.add(ADAMAlignmentFlag.MATE_NEGATIVE_STRAND);
    }
    if (samRecord.getMateUnmappedFlag()) {
      flags.add(ADAMAlignmentFlag.MATE_UNMAPPED);
    }
    if (samRecord.getNotPrimaryAlignmentFlag()) {
      flags.add(ADAMAlignmentFlag.NOT_PRIMARY_ALIGNMENT);
    }
    if (samRecord.getProperPairFlag()) {
      flags.add(ADAMAlignmentFlag.PROPER_PAIR);
    }
    if (samRecord.getReadFailsVendorQualityCheckFlag()) {
      flags.add(ADAMAlignmentFlag.READ_FAILS_VENDOR_QUALITY_CHECK);
    }
    if (samRecord.getReadPairedFlag()) {
      flags.add(ADAMAlignmentFlag.READ_PAIRED);
    }
    if (samRecord.getReadUnmappedFlag()) {
      flags.add(ADAMAlignmentFlag.READ_UNMAPPED);
    }
    if (samRecord.getFirstOfPairFlag()) {
      flags.add(ADAMAlignmentFlag.FIRST_OF_PAIR);
    }
    if (samRecord.getSecondOfPairFlag()) {
      flags.add(ADAMAlignmentFlag.SECOND_OF_PAIR);
    }
    return flags;
  }
}
