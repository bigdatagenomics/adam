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

import edu.berkeley.amplab.adam.avro.ADAMCigarOperation;
import edu.berkeley.amplab.adam.avro.ADAMCigarOperationInfo;
import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;

import java.util.ArrayList;
import java.util.List;

public class SAMtoADAMCigarConverter implements Converter<Cigar, List<ADAMCigarOperationInfo>> {

  @Override
  public List<ADAMCigarOperationInfo> convert(Cigar cigar) {
    List<ADAMCigarOperationInfo> cigarOperationInfos = new ArrayList<ADAMCigarOperationInfo>();
    for (CigarElement element : cigar.getCigarElements()) {
      CigarOperator cigarOperator = element.getOperator();
      ADAMCigarOperation adamCigarOperation = ADAMCigarOperation.UNKNOWN_OPERATION;
      switch (cigarOperator) {
      case M:
        adamCigarOperation = ADAMCigarOperation.ALIGNMENT_MATCH;
        break;
      case I:
        adamCigarOperation = ADAMCigarOperation.INSERTION_TO_THE_REFERENCE;
        break;
      case D:
        adamCigarOperation = ADAMCigarOperation.DELETION_FROM_THE_REFERENCE;
        break;
      case N:
        adamCigarOperation = ADAMCigarOperation.SKIPPED_REGION_FROM_THE_REFERENCE;
        break;
      case S:
        adamCigarOperation = ADAMCigarOperation.SOFT_CLIPPING;
        break;
      case H:
        adamCigarOperation = ADAMCigarOperation.HARD_CLIPPING;
        break;
      case P:
        adamCigarOperation = ADAMCigarOperation.PADDING;
        break;
      case EQ:
        adamCigarOperation = ADAMCigarOperation.SEQUENCE_MATCH;
        break;
      case X:
        adamCigarOperation = ADAMCigarOperation.SEQUENCE_MISMATCH;
        break;
      }
      cigarOperationInfos.add(ADAMCigarOperationInfo.newBuilder()
                                                    .setMagnitude(element.getLength())
                                                    .setOp(adamCigarOperation).build());
    }
    return cigarOperationInfos;
  }
}
