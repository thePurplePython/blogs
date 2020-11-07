***Advanced Spark Tuning, Optimization, and Performance Techniques***

***Apache Spark*** is a distributed computing big data analytics framework designed to transform, engineer, and process massive amounts of data (think terabytes and petabytes) across a cluster of machines.  It has a plethora of embedded components for specific tasks including Spark SQL’s Structured DataFrame and Structured Streaming APIs, both which will be discussed in this blog.  One of the challenges with Spark is appending new data to a data lake thus producing *'small and skewed files'* on write.  It can be tricky to solve these challenges completely, which consequently have a negative impact on users performing additional downstream Spark layers, Data Science analysis, and SQL queries consuming the *'small and skewed files'*.  A fairly new framework called ***Delta Lake*** helps address these issues.

However, in this blog using the native *Scala* API I will walk you through two Spark problem solving techniques of 1.) how to include a transient timer in your Spark *Structured Streaming* job for gracefully auto-terminating periodic data processed appends of new source data, and 2.) how to control the output size of the partitions produced by your Spark jobs.  Problem solve #1 capability avoids always paying for a long-running (sometimes idle) *'24/7'* cluster (i.e. in ***Amazon EMR***).  For example, short-lived streaming jobs are a solid option for processing only new available source data (i.e. in ***Amazon S3***) that does not have a consistent cadence arrival; perhaps landing every hour or so as mini-batches.  Problem solve #2 capability is really important for improving the I/O performance of downstream processes such as next layer Spark jobs, SQL queries, Data Science analysis, and overall data lake metadata management.

## Example 1: Spark Streaming Transient Termination Timer

1a.) Let’s view and define the schema for the public IoT device event dataset retrieved from *Databricks Community Edition* stored at ***dbfs:/databricks-datasets/structured-streaming/events/***.

```ls /blogs/source/devices.json/```

![1a-iot-dataset.png](../master/images/1a-iot-dataset.png)

```head /blogs/source/devices.json/file-0.json/```

![1a-iot-sample.png](../master/images/1a-iot-sample.png)

```scala
import org.apache.spark.sql.types.{StructType, StringType, TimestampType}

val schema = (new StructType()
              .add("time", TimestampType, true)
              .add("action", StringType, true)
              )
```

1b.) Next, we will read the dataset as a streaming dataframe with the schema defined as well as include function arguments:
- *maxFilesPerTrigger* (number of max files read per trigger)
- *basePath* (data source location)

```scala
import org.apache.spark.sql.DataFrame

def readStream(maxFilesPerTrigger: Int, basePath: String): DataFrame = {
  val readDf = spark
  .readStream
  .option("maxFilesPerTrigger", maxFilesPerTrigger)
  .option("latestFirst", true)
  .schema(schema)
  .json(basePath)
  return readDf
}
```

1c.) Now we execute the streaming query as `parquet` file sink format and `append` mode to ensure only new data is periodically written incrementally as well as include function arguments:
- *df* (source dataframe)
- *repartition* (number of persisted output partitions every trigger fire)
- *checkpointPath* (recovery checkpoint location)
- *trigger* (trigger interval processing time)
- *targetPath* (data target location)

```scala
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQuery

def writeStream(df: DataFrame, repartition: Int, checkpointPath: String, trigger: String, targetPath: String): StreamingQuery = {
  val triggerDf = df
  .repartition(repartition)
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .trigger(Trigger.ProcessingTime(trigger))
  .option("path", targetPath)
  .format("parquet")
  .outputMode("append")
  .start()
  //triggerDf.awaitTermination()
  return triggerDf
}
```

1d.) A Scala sleep function (in miliseconds) will be used to shutdown the streaming job on a graceful transient timer.

```scala
def stop(n: Int): Unit = {
  return Thread.sleep(n)
}
```

1e.)  Lastly, the streaming job spark session will be executed after the timer expires thus terminating the short-lived application.

```scala
def kill(): Unit = {
  return spark.streams.active.foreach(_.stop()); spark.stop()
}
```

1f.) Apply the functions to Scala values, and optionally set additional Spark configurations if desired:
- `spark.sql.session.timeZone` (set to `UTC` to avoid timestamp and timezone mismatch issues)
- `spark.sql.shuffle.partitions` (set to number of desired partitions created on *Wide "shuffles" Transformations* [value varies on: data volume & structure, cluster hardware & partition size, cores available, and application intentions])

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("spark-structured-streaming-transient-app").getOrCreate()
spark.conf.get("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", 2001)
val readDf = readStream(10, "/blogs/source/devices.json")
val triggerDf = writeStream(readDf, 8, "/blogs/recovery/logs.cp", "60 seconds", "/blogs/target/devices.parquet")
stop(300000) // 300000 ms = 5 min
kill()
```

1g.) View the job's output location

```ls /blogs/target/devices.parquet/```

![1g-iot-parquet-output.png](../master/images/1g-iot-parquet-output.png)

In summary, the streaming job will continuously process, convert, and append micro-batches of unprocessed data only from the source json location to the target parquet location.  After the timer runs out (ex: 5 min) a graceful shutdown of the Spark application occurs.  For Spark application deployment, best practices include defining a Scala `object` with a `main()` method including `args: Array[String]` as command line arguments.  Then create a required directory structure to compile the `<appName>.scala` (application code) file with a `build.sbt` (library dependencies) file all via *sbt* build tool to create a *JAR* file, which will be used to run the application via *spark-submit*.

Here is official **Apache Spark Documentation** explaining the steps (https://spark.apache.org/docs/latest/quick-start.html#self-contained-applications).

In AWS, via ***Amazon EMR*** you can submit applications as job steps and auto-terminate the cluster's infrastructure when all steps complete.  This can be fully orchestrated, automated, and scheduled via services like ***AWS Step Functions***, ***AWS Lambda***, and ***Amazon CloudWatch***.

Sometimes the output file size of a streaming job will be rather *'skewed'* due to a sporadic cadence arrival of the source data, as well as, the timing challenge of always syncing it with the trigger of the streaming job.  Example 2 will help address and optimize the *'small and skewed files'* dilemna.

To the next example ...

## Example 2: Spark Repartition File Size Optimization

2a.) First, let's read our input dataset (output dataset from Example #1) and identify the number of partitions in the dataframe.

```scala
import org.apache.spark.sql.DataFrame

def readParquet(basePath: String): DataFrame = {
  val parquetDf = spark
  .read
  .parquet(basePath)
  return parquetDf
}

def numPartitions(df: DataFrame): Int = {
  val num = df.rdd.getNumPartitions
  return num
}
```

2b.) In order to calculate the desired output partition (file) size you need to estimate the size (in megabytes) of the input dataframe by persisting it in memory.  This can be determined ad hoc beforehand via executing `df.cache()` or `df.persist()`, call an action like `df.count()` or `df.foreach(x => println(x))` to cache the entire dataframe, and then search for the dataframe's RAM size in the *Spark UI* under the *Storage* tab.

```scala
def ram(size: Int): Int = {
  val mb = size
  return mb
}

def target(size: Int): Int = {
  val mb = size
  return mb
}
```
