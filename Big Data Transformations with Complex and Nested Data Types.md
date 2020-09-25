Big Data Transformations with Complex and Nested Data Types
***Apache Spark*** is a distributed computing big data analytics framework designed to transform, engineer, and process massive amounts of data (think terabytes and petabytes) across a cluster of machines.  Often working with diverse datasets, you will come across complex data types and formats that require expensive compute and transformations (think *IoT* devices).  Extremely complicated and specialized, under the hood, ***Apache Spark*** is a master of its craft when it comes to scaling big data engineering efforts.  In this blog using the native *Scala* API I will walk you through examples of 1.) how to flatten and normalize semi-structured *JSON* data with nested schema (*array* and *struct*), 2.) how to pivot your data, and 3.) how to save the data to storage as *parquet* schema for downstream analytics.  To note, the same exercises can be achieved using the *Python* API and *Spark SQL*.

1a.) Let's view our beautiful multi-line *JSON* schema (dummy data from my favorite video game).

```json
[
	{
		"game": "Destiny 1",
		"nest" : [
			
			{
				"release": "d1",
				"universe": {
					"class": "Titan",
					"characteristics": {
						"subclass": ["Defender"],
						"super": ["Ward of Dawn"]
					}
				}
			},

			{
				"release": "d1",
				"universe": {
					"class": "Hunter",
					"characteristics": {
						"subclass": ["Bladedancer"],
						"super": ["Arc Blade"]
					}
				}
			},

			{
				"release": "d1",
				"universe": {
					"class": "Warlock",
					"characteristics": {
						"subclass": ["Sunsigner"],
						"super": ["Radiance"]
					}
				}
			}
		]
	},

	{
		"game": "Destiny 2",
		"nest" : [
			
			{
				"release": "d2",
				"universe": {
					"class": "Titan",
					"characteristics": {
						"subclass": ["Sunbreaker", "Sentinel", "Striker"],
						"super": ["Hammer of Sol", "Sentinel Shield", "Fist of Havoc"]
					}
				}
			},

			{
				"release": "d2",
				"universe": {
					"class": "Hunter",
					"characteristics": {
						"subclass": ["Gunslinger", "Nightstalker", "Arcstrider"],
						"super": ["Golden Gun", "Shadowshot", "Arc Staff"]
					}
				}
			},

			{
				"release": "d3",
				"universe": {
					"class": "Warlock",
					"characteristics": {
						"subclass": ["Dawnblade", "Voidwalker", "Stormcaller"],
						"super": ["Daybreak", "Nova Bomb", "Stormtrance"]
					}
				}
			}
		]
	}
]
```

1b.) Next, to improve performance I will map and construct our schema as a new ```StructType()``` to avoid triggering an unnecessary ***Spark*** job when reading the *JSON* data.

```scala
import org.apache.spark.sql.types.{StructType, StringType, ArrayType}

val schema = (new StructType()
              .add("game", StringType, true)
              .add("nest", ArrayType(new StructType()
                                     .add("release", StringType, true)
                                     .add("universe", new StructType()
                                          .add("class", StringType, true)
                                          .add("characteristics", new StructType()
                                               .add("subclass", ArrayType(StringType))
                                               .add("super", ArrayType(StringType))
                                              )
                                          )
                                     )
                   )
              )

val jsonData = "./sample.json" // path to json data directory

val nestedDf = (spark
                .read
                .option("multiline", "true")
                .schema(schema)
                .json(jsonData)
               )
```

1c.) Now we can print our schema and examine the data, which you can see is a crazy bundle of joy because of the data types involved.  There are 12 total rows with 5 columns in this dataset however we are seeing 2 rows with 2 columns in the native schema.
	
```scala
nestedDf.printSchema()
nestedDf.show(false)

root
 |-- game: string (nullable = true)
 |-- nest: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- release: string (nullable = true)
 |    |    |-- universe: struct (nullable = true)
 |    |    |    |-- class: string (nullable = true)
 |    |    |    |-- characteristics: struct (nullable = true)
 |    |    |    |    |-- subclass: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |    |-- super: array (nullable = true)
 |    |    |    |    |    |-- element: string (containsNull = true)

+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|game     |nest                                                                                                                                                                                                                                                                                           |
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Destiny 1|[[d1, [Titan, [[Defender], [Ward of Dawn]]]], [d1, [Hunter, [[Bladedancer], [Arc Blade]]]], [d1, [Warlock, [[Sunsigner], [Radiance]]]]]                                                                                                                                                        |
|Destiny 2|[[d2, [Titan, [[Sunbreaker, Sentinel, Striker], [Hammer of Sol, Sentinel Shield, Fist of Havoc]]]], [d2, [Hunter, [[Gunslinger, Nightstalker, Arcstrider], [Golden Gun, Shadowshot, Arc Staff]]]], [d2, [Warlock, [[Dawnblade, Voidwalker, Stormcaller], [Daybreak, Nova Bomb, Stormtrance]]]]]|
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

1d.) It is time to normalize this dataset using built-in ***Spark*** *DataFrame* API functions including ```explode``` , which turn elements in ```array``` data types to individual rows and dots ```*```, which unpack subfields in ```struct``` data types.  Since the *subclass* and *super* columns have a 1 to 1 element pair mapping the slick ```arrays_zip``` function will also be used with dot selection to avoid creating extra row combinations during the exploding.

```scala
import org.apache.spark.sql.functions.{explode}

val explodeRowsDf = nestedDf.select($"game", explode($"nest").as("row"))

val dotDf = (explodeRowsDf
             .select($"game",
                     $"row.release",
                     $"row.universe.*",
                     $"row.universe.characteristics.*")
             .drop("characteristics")
            )

import org.apache.spark.sql.functions.{arrays_zip}

val zipArraysDf = (dotDf
                   .select($"*", explode(arrays_zip($"subclass", $"super"))
                           .alias("arrayMap"))
                   .drop("subclass", "super")
                   )

val finalDf = (zipArraysDf
               .selectExpr("game",
                           "release",
                           "class",
                           "arrayMap.subclass",
                           "arrayMap.super")
               .orderBy($"class", $"game")
               )	 
```
	
1e.) It is time to check out the transformed dataset and its schema.  As expected, it returns 12 rows with 5 columns.

```scala
finalDf.printSchema()
finalDf.show(false)

root
 |-- game: string (nullable = true)
 |-- release: string (nullable = true)
 |-- class: string (nullable = true)
 |-- subclass: string (nullable = true)
 |-- super: string (nullable = true)

+---------+-------+-------+------------+---------------+
|game     |release|class  |subclass    |super          |
+---------+-------+-------+------------+---------------+
|Destiny 1|d1     |Hunter |Bladedancer |Arc Blade      |
|Destiny 2|d2     |Hunter |Gunslinger  |Golden Gun     |
|Destiny 2|d2     |Hunter |Arcstrider  |Arc Staff      |
|Destiny 2|d2     |Hunter |Nightstalker|Shadowshot     |
|Destiny 1|d1     |Titan  |Defender    |Ward of Dawn   |
|Destiny 2|d2     |Titan  |Striker     |Fist of Havoc  |
|Destiny 2|d2     |Titan  |Sentinel    |Sentinel Shield|
|Destiny 2|d2     |Titan  |Sunbreaker  |Hammer of Sol  |
|Destiny 1|d1     |Warlock|Sunsigner   |Radiance       |
|Destiny 2|d2     |Warlock|Dawnblade   |Daybreak       |
|Destiny 2|d2     |Warlock|Voidwalker  |Nova Bomb      |
|Destiny 2|d2     |Warlock|Stormcaller |Stormtrance    |
+---------+-------+-------+------------+---------------+
```

2a.)  This next exercise will take our flattened dataset and apply ```pivot``` functions, which trigger a wide transformation where distinct values for a specific column are transposed into individual columns.  Pivots can be performed with or without aggregation.  Without aggregation is often a required schema for data science use cases using many columns *a.k.a* features as input to learning algorithms.  An efficient performance tip is to specify your unique values in the ```pivot``` function input so ***Spark*** does not have to trigger an additional job.

```scala
val noAggPivotDf = (finalDf
                  .groupBy($"game")
                  .pivot($"class", Seq("Hunter", "Titan", "Warlock"))
                  .agg(expr("first(super)"))
                   )

aggPivotDf.show()

+---------+----------+-------------+--------+
|     game|    Hunter|        Titan| Warlock|
+---------+----------+-------------+--------+
|Destiny 1| Arc Blade| Ward of Dawn|Radiance|
|Destiny 2|Golden Gun|Hammer of Sol|Daybreak|
+---------+----------+-------------+--------+

val aggPivotDf = (finalDf
                    .groupBy($"class")
                    .pivot($"game", Seq("Destiny 1", "Destiny 2"))
                    .agg(expr("count(subclass)"))
                 )

+-------+---------+---------+
|  class|Destiny 1|Destiny 2|
+-------+---------+---------+
|  Titan|        1|        3|
| Hunter|        1|        3|
|Warlock|        1|        3|
+-------+---------+---------+
```

3a.)  The final exercise will simply write out our data to storage.  *Parquet* format will be used because it is a splittable file format, highly compressed for space efficiency, and optimized for columnar storage hence making it fabulous for downstream big data analytics.

```scala
aggPivotDf.write.format("parquet").save("./table.parquet") // path to parquet directory
val readDf = spark.read.parquet("./table.parquet")

readDf.printSchema()
readDf.show()

root
 |-- game: string (nullable = true)
 |-- Hunter: string (nullable = true)
 |-- Titan: string (nullable = true)
 |-- Warlock: string (nullable = true)

+---------+----------+-------------+--------+
|     game|    Hunter|        Titan| Warlock|
+---------+----------+-------------+--------+
|Destiny 2|Golden Gun|Hammer of Sol|Daybreak|
|Destiny 1| Arc Blade| Ward of Dawn|Radiance|
+---------+----------+-------------+--------+
```

These exercises just scratch the surface of what ***Apache Spark*** is capable of for big data engineering and advanced analytics use cases.  Thank you for reading this blog.  Please reach out with any questions, interests, collaboration, and or feedback.
 // garrett r peternel
