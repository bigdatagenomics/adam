# Building Downstream Applications

ADAM is packaged so that it can be used via the base CLI, to run plugins via the CLI, or as a library which can be used when building downstream applications. This document covers some of the important things to note when building applications downstream of ADAM.

## Creating a Spark Context

The SparkContext is the main class through which applications are controlled in Spark, and a Spark Context is necessary to use ADAM's Spark based transformation API. If you are using ADAM's CLI, you don't need to create a Spark Context; the CLI will do that for you. The main concern when creating a Spark Context is that you will need to ensure that the necessary object serializers are registered. There are four ways to create a [SparkContext](https://spark.apache.org/docs/1.0.0/api/scala/index.html#org.apache.spark.SparkContext) that will work with ADAM:

1. Implement the [SparkCommand](https://github.com/bigdatagenomics/adam/tree/master/adam-cli/src/main/scala/org/bdgenomics/adam/cli/ADAMCommand.scala) trait. This will provide you with a Spark Context, with all of the serializers registered, as well as a Hadoop Job.
1. Implement an [ADAMPlugin](https://github.com/bigdatagenomics/adam/tree/master/adam-core/src/main/scala/org/bdgenomics/adam/plugins/ADAMPlugin.scala); documents TBA. 
1. Create a SparkContext via [ADAMContext.createSparkContext](https://github.com/bigdatagenomics/adam/tree/master/adam-core/src/main/scala/org/bdgenomics/adam/rdd/ADAMContext.scala). This is the same method used by the SparkCommand trait and the ADAMPlugin to create a SparkContext.
1. Create SparkContext via the SparkContext class, and then manually register serializers. This isn't recommended unless you are a Spark power user, as this is more complex, and more fraught with the possibility of error. If you would like to take this path, please find the ADAM serializer registration code [here](https://github.com/bigdatagenomics/adam/tree/master/adam-core/src/main/scala/org/bdgenomics/adam/serialization).

## Building for a specific Hadoop version

By default, ADAM depends on [Spark](http://spark.apache.org) 1.0.0 and [Hadoop](http://hadoop.apache.org) 2.2.0. With a few changes, ADAM can be used under other versions of Hadoop. Currently, we have tested ADAM on Hadoop 1.0.4 through Hadoop 2.4.0, and our [automated test infrastructure](http://amplab.cs.berkeley.edu/jenkins/job/adam) runs test builds for Hadoop 1.0.4, 2.2.0, and 2.3.0.

If you are building a downstream project that includes ADAM, and you want to build for a non-2.2.0 Hadoop version and are using [Maven](http://maven.apache.org) or SBT, you will need to add an explicit dependency on the hadoop-client artifact for the version of Hadoop you want to build for. In this dependency, you will also need to exclude the `javax.servlet` class. In a Maven pom.xml, this will look like:

```
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-client</artifactId>
  <version>version.you.want</version>
  <exclusions>
    <exclusion>
      <groupId>javax.servlet</groupId>
      <artifactId>*</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```
