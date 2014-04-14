package org.bdgenomics.adam.rdd.features

import org.apache.spark.{SparkContext, Logging}
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.adam.avro.ADAMFeature
import org.apache.spark.rdd.RDD

object ADAMFeaturesContext {
  implicit def sparkContextToADAMFeaturesContext(sc: SparkContext): ADAMFeaturesContext = new ADAMFeaturesContext(sc)
}

class ADAMFeaturesContext(sc: SparkContext) extends Serializable with Logging {

  // TODO(laserson): what's the correct way to parametrize this?
  def adamFeatureLoad[FT <: ADAMFeature](filePath: String): RDD[FT] = {
    val job = HadoopUtil.newJob(sc)
    val extensionPattern = """.*[.]([^.]*)""".r
    val extensionPattern(extension) = filePath
    extension.toLowerCase match {
      case "bed" => sc.textFile(filePath).map(new BEDParser().parse _)
      case "narrowPeak" => sc.textFile(filePath).map(new NarrowPeakParser().parse _)
    }
  }
}
