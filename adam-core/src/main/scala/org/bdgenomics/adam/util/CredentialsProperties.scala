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
package org.bdgenomics.adam.util

import java.io.{ FileInputStream, File }
import org.apache.spark.Logging
import scala.io.Source
import com.amazonaws.auth.AWSCredentials

/**
 * CredentialsProperties is a wrapper class which extracts Amazon S3 keys (although it could be
 * modified to extract other key / secret-key pairs) from a key-value-formatted file (see below),
 * if available, or from the System environment otherwise.
 *
 * (This is to make testing and running on a local environment easier.)
 *
 * The 'location' parameter names a file which should have the format of
 *   key=value
 * one per line.  Whitespace around both 'key' and 'value' are stripped.
 *
 * @param location The location of the file which will be read _if it exists and is readable_,
 *                 otherwise the parameters will be read from the System environment.
 */
class CredentialsProperties(location: File) extends Serializable with Logging {

  val defaultAccessKey = System.getenv("AWS_ACCESS_KEY_ID")
  val defaultSecretKey = System.getenv("AWS_SECRET_KEY")
  private val defaultMap = Map("accessKey" -> defaultAccessKey, "secretKey" -> defaultSecretKey)
  val configuration = new ConfigurationFile(location, Some(defaultMap))

  /**
   * Retrieves the value associated with a given key from the configuration file.
   *
   * The optional 'suffix' argument is used for differentiating different key/value pairs,
   * used for different purposes but with similar keys.  For example, we (Genome Bridge)
   * will create configuration files with one key 'accessKey' which contains the AWS access key
   * for accessing almost all our AWS resources -- however, we use a different access (and secret)
   * key for accessing S3, and that's put into the same configuration file with a key name
   * 'accessKey_s3'.  (Similarly for 'secretKey' and 'secretKey_s3').
   *
   * @param keyName The base key used to retrieve a value
   * @param suffix If suffix is None, then the base key is used to retrieve the corresponding
   *               value.  If suffix is specified, then '[base key]_[suffix]' is used instead.
   *               If '[base key]_[suffix]' doesn't exist in the file, then the properties
   *               falls back to retrieving the value just associated with the base key itself.
   * @return The (string) value associated with the given key, or null if no such key exists.
   */
  def configuredValue(keyName: String, suffix: Option[String] = None): String = {
    suffix match {
      case None => configuration.properties(keyName)
      case Some(suffixString) => {
        val combinedKey = "%s_%s".format(keyName, suffixString)
        if (configuration.properties.contains(combinedKey)) {
          configuration.properties(combinedKey)
        } else {
          configuration.properties(keyName)
        }
      }
    }
  }

  def accessKey(suffix: Option[String]): String = configuredValue("accessKey", suffix)
  def secretKey(suffix: Option[String]): String = configuredValue("secretKey", suffix)

  def awsCredentials(suffix: Option[String] = None): AWSCredentials = {
    new SerializableAWSCredentials(accessKey(suffix), secretKey(suffix))
  }
}

private[util] case class ConfigurationFile(properties: Map[String, String]) extends Serializable {
  def this(f: File, defaultValues: Option[Map[String, String]] = None) = this(ConfigurationParser(f, defaultValues))
}

private[util] object ConfigurationParser extends Logging {

  def apply(f: File, defaultValues: Option[Map[String, String]] = None): Map[String, String] = {
    if (!f.exists() || !f.canRead) {
      logWarning("File \"%s\" does not exist, using default values.".format(f.getAbsolutePath))
      defaultValues.get

    } else {
      logInfo("Reading configuration values from \"%s\"".format(f.getAbsolutePath))
      val is = new FileInputStream(f)
      // we wrote our own key=value parsing, since we were having problems with whitespace
      // using the Java util.Properties parsing.
      val lines = Source.fromInputStream(is).getLines().map(_.trim)
      val nonComments = lines.filter(line => !line.startsWith("#") && line.contains("="))
      val map = nonComments.map(_.split("=")).map(array => array.map(_.trim)).map(array => (array(0), array(1))).toMap
      is.close()
      map
    }
  }
}
