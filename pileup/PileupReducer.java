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

package edu.berkeley.cs.amplab.adam.modules.pileup;

import edu.berkeley.cs.amplab.adam.avro.ADAMPileup;
import edu.berkeley.cs.amplab.adam.avro.ReferencePosition;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PileupReducer extends Reducer<AvroKey<ReferencePosition>, AvroValue<ADAMPileup>,
        Void, ADAMPileup> {

    @Override
    protected void reduce(AvroKey<ReferencePosition> referencePosition, Iterable<AvroValue<ADAMPileup>> pileups,
                          Context context) throws IOException, InterruptedException {
        for (AvroValue<ADAMPileup> pileup : pileups) {
            context.write(null, pileup.datum());
        }
    }
}
