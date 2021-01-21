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
package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }
import org.seqdoop.hadoop_bam.CRAMInputFormat

/**
 * Abstract arguments that capture CRAM format configuration.
 */
private[cli] trait CramArgs {

  @Args4jOption(required = false, name = "-cram_reference", usage = "CRAM format reference, if necessary")
  var cramReference: String = null

  /**
   * Configure CRAM format.
   *
   * @param sc Spark context to configure
   */
  def configureCramFormat(sc: SparkContext) = {
    Option(cramReference).map(ref => {
      sc.hadoopConfiguration.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, ref)
    })
  }
}
