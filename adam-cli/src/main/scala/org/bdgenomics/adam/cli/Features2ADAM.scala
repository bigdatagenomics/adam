package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.avro.ADAMFeature
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.features.ADAMFeaturesContext._
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.Argument

object Features2ADAM extends ADAMCommandCompanion {
  val commandName = "features2adam"
  val commandDescription = "Convert a file with sequence features into corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Features2ADAM(Args4j[Features2ADAMArgs](cmdline))
  }
}

class Features2ADAMArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "FEATURES",
    usage = "The features file to convert (e.g., .bed, .gff)", index = 0)
  var featuresFile: String = _
  @Argument(required = true, metaVar = "ADAM",
    usage = "Location to write ADAM features data")
  var outputPath: String = null
}

class Features2ADAM(val args: Features2ADAMArgs)
    extends ADAMSparkCommand[Features2ADAMArgs] {
  val companion = Features2ADAM

  def run(sc: SparkContext, job: Job) {
    val features: RDD[ADAMFeature] = sc.adamFeatureLoad(args.featuresFile)
    features.adamSave(args.outputPath, blockSize = args.blockSize,
      pageSize = args.pageSize, compressCodec = args.compressionCodec,
      disableDictionaryEncoding = args.disableDictionary)
  }
}
