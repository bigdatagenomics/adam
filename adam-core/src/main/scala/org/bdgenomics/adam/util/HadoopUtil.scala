/*
 * Copyright (c) 2014. Regents of the University of California
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
package org.bdgenomics.adam.util

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.conf.Configuration

object HadoopUtil {

  def newJob(): Job = {
    newJob(new Configuration())
  }

  def newJob(config: Configuration): Job = {
    val jobClass: Class[_] = Class.forName("org.apache.hadoop.mapreduce.Job")
    try {
      // Use the getInstance method in Hadoop 2
      jobClass.getMethod("getInstance", classOf[Configuration]).invoke(null, config).asInstanceOf[Job]
    } catch {
      case ex: NoSuchMethodException =>
        // Drop back to Hadoop 1 constructor
        jobClass.getConstructor(classOf[Configuration]).newInstance(config).asInstanceOf[Job]
    }
  }

  /**
   * Create a job using either the Hadoop 1 or 2 API
   * @param sc A Spark context
   */
  def newJob(sc: SparkContext): Job = {
    newJob(sc.hadoopConfiguration)
  }

  /**
   * In Hadoop 2.x, isDir is deprecated in favor of isDirectory
   * @param fs
   * @return
   */
  def isDirectory(fs: FileStatus): Boolean = {
    val fsClass: Class[_] = fs.getClass
    try {
      // Use the isDirectory method in Hadoop 2
      fsClass.getMethod("isDirectory").invoke(fs).asInstanceOf[Boolean]
    } catch {
      case ex: NoSuchMethodException =>
        // Drop back to Hadoop 1 isDir method
        fsClass.getMethod("isDir").invoke(fs).asInstanceOf[Boolean]
    }
  }

}
