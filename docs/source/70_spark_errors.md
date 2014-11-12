# Common Spark Errors

## Avro GenericData ClassCastException

This error indicates that the type was not referenced from the classpath provided for the JVM. In this case, the Avro deserializer returns a GenericData$Record instead of the specific type that was provided. An example of this error:

```
java.lang.ClassCastException: org.apache.avro.generic.GenericData$Record cannot be cast to edu.berkeley.cs.amplab.adam.avro.ADAMRecord
        at edu.berkeley.cs.amplab.adam.rdd.AdamContext$$anonfun$2.apply(AdamContext.scala:191)
        at edu.berkeley.cs.amplab.adam.rdd.AdamContext$$anonfun$2.apply(AdamContext.scala:191)
        at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
        at scala.collection.Iterator$$anon$14.hasNext(Iterator.scala:389)
        at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:327)
        at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:327)
        at scala.collection.Iterator$class.foreach(Iterator.scala:727)
        at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
        at scala.collection.generic.Growable$class.$plus$plus$eq(Growable.scala:48)
        at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:103)
        at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:47)
        at scala.collection.TraversableOnce$class.to(TraversableOnce.scala:273)
        at scala.collection.AbstractIterator.to(Iterator.scala:1157)
        at scala.collection.TraversableOnce$class.toBuffer(TraversableOnce.scala:265)
        at scala.collection.AbstractIterator.toBuffer(Iterator.scala:1157)
        at scala.collection.TraversableOnce$class.toArray(TraversableOnce.scala:252)
        at scala.collection.AbstractIterator.toArray(Iterator.scala:1157)
        at org.apache.spark.rdd.RDD$$anonfun$4.apply(RDD.scala:602)
        at org.apache.spark.rdd.RDD$$anonfun$4.apply(RDD.scala:602)
        at org.apache.spark.SparkContext$$anonfun$runJob$4.apply(SparkContext.scala:884)
        at org.apache.spark.SparkContext$$anonfun$runJob$4.apply(SparkContext.scala:884)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:109)
        at org.apache.spark.scheduler.Task.run(Task.scala:53)
        at org.apache.spark.executor.Executor$TaskRunner$$anonfun$run$1.apply$mcV$sp(Executor.scala:213)
        at org.apache.spark.deploy.SparkHadoopUtil.runAsUser(SparkHadoopUtil.scala:49)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:178)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
        at java.lang.Thread.run(Thread.java:744)
```

If this is occurring on the Spark cluster, the ```SPARK_CLASSPATH``` variable may not be set properly in the environment. This should be set to a location that is known to contain the jar on all of the nodes in the cluster.
