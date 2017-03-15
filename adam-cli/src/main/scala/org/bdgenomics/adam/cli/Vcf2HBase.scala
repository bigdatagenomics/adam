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
import org.bdgenomics.adam.hbase.HBaseFunctions
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SequenceDictionary, SnpTable }
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Filter }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.{ AlignedReadRDD, AlignmentRecordRDD, MDTagging }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Vcf2HBase extends BDGCommandCompanion {
  val commandName = "Vcf2HBase"
  val commandDescription = "Write a VCF file to HBase"

  def apply(cmdLine: Array[String]) = {
    new Vcf2HBase(Args4j[Vcf2HBaseArgs](cmdLine))
  }
}

class Vcf2HBaseArgs extends Args4jBase {

  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var vcfPath: String = _

  @Argument(required = true, metaVar = "HBASE_TABLE", usage = "HBase table name in which to load VCF file")
  var hbaseTable: String = null

  @Args4jOption(required = true, name = "-seq_dict_id", usage = "User defined name to apply to the sequence dictionary create from this VCF, or name of existing sequence dictionary to be used")
  var seqDictId: String = null

  @Args4jOption(required = false, name = "-use_existing_seqdict", usage = "Use an existing sequence dictionary, don't write a new one")
  var useExistingSeqDict: Boolean = false

  @Args4jOption(required = false, name = "-repartition", usage = "Repartition into N partitions prior to writing to HBase")
  var repartitionNum: Int = 0

  @Argument(required = true, metaVar = "STAGING_FOLDER", usage = "Location for temporary files during bulk load")
  var stagingFolder: String = null
}

class Vcf2HBase(protected val args: Vcf2HBaseArgs) extends BDGSparkCommand[Vcf2HBaseArgs] with Logging {
  val companion = Vcf2HBase

  def run(sc: SparkContext) {

    val vcRdd = sc.loadVcf(args.vcfPath)

    val hbaseConnection = new HBaseFunctions.HBaseDataAccessObject(sc)
    HBaseFunctions.saveVariantContextRDDToHBaseBulk(hbaseConnection,
      vcRdd,
      args.hbaseTable,
      args.seqDictId,
      args.stagingFolder,
      args.useExistingSeqDict,
      Some(args.repartitionNum))

  }

}
