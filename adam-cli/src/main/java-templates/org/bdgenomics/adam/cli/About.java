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
package org.bdgenomics.adam.cli;

/**
 * About.
 */
public final class About {
    private static final String ARTIFACT_ID = "${project.artifactId}";
    private static final String BUILD_TIMESTAMP = "${build-helper-maven-plugin.build.timestamp}";
    private static final String COMMIT = "${git.commit.id}";
    private static final String HADOOP_VERSION = "${hadoop.version}";
    private static final String SCALA_VERSION = "${scala.version}";
    private static final String SPARK_VERSION = "${spark.version}";
    private static final String VERSION = "${project.version}";

    /**
     * Return the artifact id.
     *
     * @return the artifact id
     */
    public String artifactId() {
        return ARTIFACT_ID;
    }

    /**
     * Return the build timestamp.
     *
     * @return the build timestamp
     */
    public String buildTimestamp() {
        return BUILD_TIMESTAMP;
    }

    /**
     * Return the last commit.
     *
     * @return the last commit
     */
    public String commit() {
        return COMMIT;
    }

    /**
     * Return the Hadoop compile scope dependency version.
     *
     * @return the Hadoop compile scope dependency version
     */
    public String hadoopVersion() {
        return HADOOP_VERSION;
    }

    /**
     * Return the Scala compile scope dependency version.
     *
     * @return the Scala compile scope dependency version
     */
    public String scalaVersion() {
        return SCALA_VERSION;
    }

    /**
     * Return the Apache Spark compile scope dependency version.
     *
     * @return the Apache Spark compile scope dependency version
     */
    public String sparkVersion() {
        return SPARK_VERSION;
    }

    /**
     * Return the ADAM version.
     *
     * @return the ADAM version
     */
    public String version() {
        return VERSION;
    }

    /**
     * Return true if the ADAM version is a snapshot.
     *
     * @return true if the ADAM version is a snapshot
     */
    public boolean isSnapshot() {
        return VERSION.contains("SNAPSHOT");
    }
}
