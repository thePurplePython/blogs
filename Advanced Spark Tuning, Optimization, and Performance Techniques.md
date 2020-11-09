***Advanced Spark Tuning, Optimization, and Performance Techniques***

## Introduction

***Apache Spark*** is a distributed computing big data analytics framework designed to transform, engineer, and process massive amounts of data (think terabytes and petabytes) across a cluster of machines.  It has a plethora of embedded components for specific tasks including Spark SQL’s Structured DataFrame and Structured Streaming APIs, both which will be discussed in this blog.  One of the challenges with Spark is appending new data to a data lake thus producing *'small and skewed files'* on write.  It can be tricky to solve these challenges completely, which consequently have a negative impact on users performing additional downstream Spark layers, Data Science analysis, and SQL queries consuming the *'small and skewed files'*.  Fairly new framework ***Delta Lake*** and ***Apache Hudi*** help address these issues.

However, in this blog using the native Scala API I will walk you through two Spark problem solving techniques of 1.) how to include a transient timer in your Spark *Structured Streaming* job for gracefully auto-terminating periodic data processing appends of new source data, and 2.) how to control the number of output files and the size of the partitions produced by your Spark jobs.  Problem solve #1 capability avoids always paying for a long-running (sometimes idle) *'24/7'* cluster (i.e. in *Amazon EMR*).  For example, short-lived streaming jobs are a solid option for processing only new available source data (i.e. in *Amazon S3*) that does not have a consistent cadence arrival; perhaps landing every hour or so as mini-batches.  Problem solve #2 capability is really important for improving the I/O performance of downstream processes such as next layer Spark jobs, SQL queries, Data Science analysis, and overall data lake metadata management.

***Disclaimer:  The public datasets used in this blog contain very small data volumes and are used for demonstration purposes only.  These Spark techniques are best applied on real-world big data volumes (i.e. terabytes & petabytes).  Hence, size, configure, and tune Spark clusters & applications accordingly.***

## Example 1: Spark Streaming Transient Termination Timer

1a.) First, let’s view some sample files and define the schema for the public IoT device event dataset retrieved from *Databricks Community Edition* stored at *dbfs:/databricks-datasets/structured-streaming/events/*.

```ls /blogs/source/devices.json/```

![1a-iot-dataset-json-input.png](../master/images/1a-iot-dataset-json-input.png)

```head /blogs/source/devices.json/file-0.json/```

![1a-iot-sample.png](../master/images/1a-iot-sample.png)

```scala
import org.apache.spark.sql.types.{StructType, StringType, TimestampType}

val schema = (new StructType()
              .add("time", TimestampType, true)
              .add("action", StringType, true)
              )
```

1b.) Next, we will read the dataset as a streaming dataframe with the schema defined, as well as, include function arguments:
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

1c.) Now, we execute the streaming query as `parquet` file sink format and `append` mode to ensure only new data is periodically written incrementally, as well as, include function arguments:
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

1d.) A Scala sleep function (in milliseconds) will be used to shutdown the streaming job on a graceful transient timer.

```scala
def stop(n: Int): Unit = {
  return Thread.sleep(n)
}
```

1e.)  Lastly, the streaming job Spark Session will be executed after the timer expires thus terminating the short-lived application.

```scala
def kill(): Unit = {
  return spark.streams.active.foreach(_.stop()); spark.stop()
}
```

1f.) Apply the functions to Scala values, and optionally set additional Spark properties if needed:
- `spark.sql.session.timeZone` (set to *UTC* to avoid timestamp and timezone mismatch issues)
- `spark.sql.shuffle.partitions` (set to number of desired partitions created on *Wide 'shuffles' Transformations*; value varies on things like: 1. data volume & structure, 2. cluster hardware & partition size, 3. cores available, 4. application's intention)

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("spark-structured-streaming-transient-app").getOrCreate()
spark.conf.get("spark.sql.session.timeZone", "UTC")  // example
spark.conf.set("spark.sql.shuffle.partitions", 2001) // example
val readDf = readStream(10, "/blogs/source/devices.json")
val triggerDf = writeStream(readDf, 8, "/blogs/recovery/logs.cp", "60 seconds", "/blogs/target/devices.parquet")
stop(300000) // 300000 ms = 5 min
kill()
```

1g.) View the job's output location

```ls /blogs/target/devices.parquet/```

![1g-iot-dataset-parquet-output.png](../master/images/1g-iot-dataset-parquet-output.png)

In summary, the streaming job will continuously process, convert, and append micro-batches of unprocessed data only from the source json location to the target parquet location.  After the timer runs out (ex: 5 min) a graceful shutdown of the Spark application occurs.  For Spark application deployment, best practices include defining a Scala `object` with a `main()` method including `args: Array[String]` as command line arguments.  Then create a required directory structure to compile the `<appName>.scala` (application code) file with a `build.sbt` (library dependencies) file all via *SBT* build tool to create a *JAR* file, which will be used to run the application via `spark-submit`.

Here is official **Apache Spark Documentation** explaining the steps (https://spark.apache.org/docs/latest/quick-start.html#self-contained-applications).

In AWS, via *Amazon EMR* you can submit applications as job steps and auto-terminate the cluster's infrastructure when all steps complete.  This can be fully orchestrated, automated, and scheduled via services like *AWS Step Functions*, *AWS Lambda*, and *Amazon CloudWatch*.

Sometimes the output file size of a streaming job will be rather *'skewed'* due to a sporadic cadence arrival of the source data, as well as, the timing challenge of always syncing it with the trigger of the streaming job.  Example 2 will help address and optimize the *'small and skewed files'* dilemma.

To the next example ...

## Example 2: Spark Repartition File Size Optimization

2a.) First, let's view some sample files and read our public airlines input dataset (retrieved from *Databricks Community Edition* stored at *dbfs:/databricks-datasets/airlines/* and converted to small parquet files for demo purposes) and identify the number of partitions in the dataframe.

```ls /blogs/source/airlines.parquet/```

![2a-airlines-dataset-parquet-input.png](../master/images/2a-airlines-dataset-parquet-input.png)

```display(df)```

![2a-airlines-sample.png](../master/images/2a-airlines-sample.png)

```scala
import org.apache.spark.sql.DataFrame

def readParquet(basePath: String): DataFrame = {
  val parquetDf = spark
  .read
  .parquet(basePath)
  return parquetDf
}

def num(df: DataFrame): Int = {
  val numPartitions = df.rdd.getNumPartitions
  return numPartitions
}
```

2b.) In order to calculate the desired output partition (file) size you need to estimate the size (i.e. megabytes) of the input dataframe by persisting it in memory.  This can be determined ad hoc beforehand via executing `df.cache()` or `df.persist()`, call an action like `df.count()` or `df.foreach(x => println(x))` to cache the entire dataframe, and then search for the dataframe's RAM size in the *Spark UI* under the *Storage* tab.

```scala
def ram(size: Int): Int = {
  val ramMb = size
  return ramMb
}

def target(size: Int): Int = {
  val targetMb = size
  return targetMb
}
```

2c.) The Spark property `spark.default.parallelism` can help with determining the initial partitioning of a dataframe, as well as, be used to increase Spark parallelism.  Generally it is recommended to set this parameter to the number of available cores in your cluster times 2 or 3.  For example, in *Databricks Community Edition* the `spark.default.parallelism` is only 8 (*Local Mode* single machine with 1 Spark executor and 8 total cores).  For real-world scenarios, I recommend you avoid trying to set this application parameter at runtime or in a notebook.  In *Amazon EMR*, you can attach a configuration file when creating the Spark cluster's infrastructure and thus achieve more parallelism using this formula ```spark.default.parallelism = spark.executor.instances * spark.executors.cores * 2 (or 3)```.  For review, the ```spark.executor.instances``` property is the total number of JVM containers across worker nodes.  Each executor has a universal fixed amount of allocated internal cores set via the ```spark.executor.cores``` property.

*'Cores'* are also known as *'slots'* or *'threads'* and are responsible for executing Spark *'tasks'* in parallel, which are mapped to Spark *'partitions'* also known as a *'chunk of data in a file'*.

Here is official **Apache Spark Documentation** explaining the many properties (https://spark.apache.org/docs/latest/configuration.html).

```scala
def dp(): Int = {
  val defaultParallelism  = spark.sparkContext.defaultParallelism
  return defaultParallelism
}

def files(dp: Int, multiplier: Int, ram: Int, target: Int): Int = {
  val maxPartitions = Math.max(dp * multiplier, Math.ceil(ram / target).toInt)
  return maxPartitions
}
```

2d.)  The new dataframe's partition value will be determined on which integer value is larger: *(defaultParallelism times multiplier)* or *(approx. dataframe memory size divided by approx. desired partition size)*.

```scala
def split(df: DataFrame, max: Int): DataFrame = {
  val repartitionDf = df.repartition(max)
  return repartitionDf
}

def writeParquet(df: DataFrame, targetPath: String) {
  return df.write.format("parquet").mode("overwrite").save(targetPath)
}
```

2e.)  For demonstration, the cached dataframe is approximately 3,000 mb and a desired partition size is 128 mb.  In this example, the calculated partition size *(3,000 divided by 128=~23)* is greater than the default parallelism multiplier *(8 times 2=16)* hence why the value of 23 was chosen as the repartitioned dataframe's new partition count to spit on.

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("spark-repartition-optimizer-app").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 2001) // example
val parquetDf = readParquet("/blogs/source/airlines.parquet/")
val numPartitions = num(parquetDf)
val ramMb = ram(3000) // approx. df cache size
val targetMb = target(128) // approx. partition size (between 50 and 200 mb)
val defaultParallelism = dp()
val maxPartitions = files(defaultParallelism, 2, ramMb, targetMb)
val repartitionDf = split(parquetDf, maxPartitions)
writeParquet(repartitionDf, "/blogs/optimized/airlines.parquet/")
```

2f.)  Lastly, we view some sample output partitions and can see there are exactly 23 files (*part-00000* to *part-00022*) approximately 127 mb (~127,000,000 bytes = ~127 mb) each in size, which is close to the set 128 mb target size, as well as, within the optimized 50 to 200 mb recommendation.  Having the same optimized file size across all partitions solves the *'small and skewed files'* problem that harms data lake management, storage costs, and analytics I/O performance.  Alternatives include partitioning the data by columns too.  For example, a folder hierarchy (i.e. *year / month / day)* containing 1 merged partition per day.  Specific best practices will vary and depend on use case requirements, data volume, and data structure though.

```ls /blogs/optimized/airlines.parquet/```

![2f-airlines-parquet-opt-output.png](../master/images/2f-airlines-parquet-opt-output.png)

## Conclusion

In perspective, hopefully you can see that Spark properties like ```spark.sql.shuffle.partitions``` and ```spark.default.parallelism``` 
have a significant impact on the performance of your Spark applications.  It is critical these kind of Spark properties are tuned accordingly to optimize the output number and size of the partitions when processing large datasets across many Spark worker nodes.

In summary, these kind of Spark techniques have worked for me on many occasions when building out highly available and fault tolerant data lakes, resilient machine learning pipelines, cost-effective cloud compute and storage savings, and optimal I/O for generating a reusable curated feature engineering repository.  However, they may or may not be official best practices within the Spark community.  The benefits will likely depend on your use case.  In addition, exploring these various types of tuning, optimization, and performance techniques have tremendous value and will help you better understand the internals of Spark.  Creativity is one of the best things about open source software and cloud computing for continuous learning, solving real-world problems, and delivered solutions.  Thank you for reading this blog.
