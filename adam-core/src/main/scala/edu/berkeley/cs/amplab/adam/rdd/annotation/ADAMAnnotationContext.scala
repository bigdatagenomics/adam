/*
 * Copyright (c) 2014. Cloudera Inc.
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

package edu.berkeley.cs.amplab.adam.rdd.annotation

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMAnnotation

object ADAMAnnotationContext {
  implicit def sparkContextToADAMAnnotationContext(sc: SparkContext): ADAMAnnotationContext = new ADAMAnnotationContext(sc)
}

class ADAMAnnotationContext(sc: SparkContext) extends Serializable with Logging {
  def adamAnnotLoad(filePath: String, metadata: AnnotationMetadata): RDD[ADAMAnnotation] = {
    log.info("Reading annotation file %s to create RDD[ADAMAnnotation]".format(filePath))
    val extensionPattern = """.*[.]([^.]*)""".r
    val extensionPattern(extension) = filePath
    extension.toLowerCase match {
      case "bed" => sc.textFile(filePath).filter(!_.startsWith("track")).map(new BEDParser(metadata).parse _)
      case "gff" => sc.textFile(filePath).map(new GFF2Parser(metadata).parse _)
      case "narrowpeak" => sc.textFile(filePath).filter(!_.startsWith("track")).map(new NarrowPeakParser(metadata).parse _)
    }
  }
}
