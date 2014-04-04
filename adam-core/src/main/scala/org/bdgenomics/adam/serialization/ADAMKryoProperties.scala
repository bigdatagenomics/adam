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

package org.bdgenomics.adam.serialization

object ADAMKryoProperties {

  /**
   * Sets up serialization properties for ADAM.
   *
   * @param kryoBufferSize Buffer size in MB to allocate for object serialization. Default is 4MB.
   */
  def setupContextProperties(kryoBufferSize: Int = 4) = {
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
    System.setProperty("spark.kryoserializer.buffer.mb", kryoBufferSize.toString)
    System.setProperty("spark.kryo.referenceTracking", "false")
  }
}
