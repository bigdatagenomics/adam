/*
 * Copyright (c) 2014 The Regents of the University of California
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

package edu.berkeley.cs.amplab.adam.rdd.recalibration

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.DecadentRead
import edu.berkeley.cs.amplab.adam.rich.DecadentRead._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._

class Recalibrator(val covariates: CovariateSpace, val table: RecalibrationTable)
  extends (DecadentRead => ADAMRecord) with Serializable {

  def apply(read: DecadentRead): ADAMRecord = {
    // TODO: recalibrate
    ADAMRecord.newBuilder(read.record).build()
  }
}

object Recalibrator {
  def apply(observed: ObservationTable): Recalibrator = {
    new Recalibrator(observed.covariates, RecalibrationTable(observed))
  }
}

class RecalibrationTable {
}

object RecalibrationTable {
  def apply(observed: ObservationTable): RecalibrationTable = {
    // TODO: compute marginals
    new RecalibrationTable
  }
}
