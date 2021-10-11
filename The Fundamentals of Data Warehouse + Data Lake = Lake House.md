***The Fundamentals of Data Warehouse + Data Lake = Lake House***

# Introduction
With the evolution of Data Warehouses and Data Lakes, they have certainly become more specialized yet siloed in their respective landscapes over the last few years. Both data management technologies each have their own identities and are best used for certain tasks and needs, however they also struggle in providing some important abilities. Data Warehouse advantages are focused around analyzing structured data, OLTP, schema-on-write, SQL, and delivering ACID-compliant database transactions. Data Lake advantages are focused around analyzing all types of data (structured, semi-structured, unstructured), OLAP, schema-on-read, API connectivity, and low-cost object storage systems for data in open file formats (i.e. Apache Parquet).

Notably, Data Warehouses particularly struggle with support for advanced data engineering, data science, and machine learning. For example, their inability to store unstructured data (i.e. text, images, video, feature engineering vectors, etc.) for machine learning development. In addition, proprietary Data Warehouse software are expensive and struggle with integrating open source + cloud platform data science and data engineering tools (i.e. Python, Scala, Spark, SageMaker, Anaconda, DataRobot, SAS, R, etc.) for exploratory data analysis via notebooks, distributed compute processing, hosting deployed models, and storing inference pipeline results. System integration, data movement costs, and data staleness will even become more challenging (especially with limited technology choices at your disposal) to address in a hybrid on-premise cloud environment.

On the flip side, unfortunately, Data Lakes sometimes notoriously struggle with data quality, transactional support, data governance, and query performance issues. Data Lakes built without vital skills, key capabilities, and specialized technologies will inevitably over time turn into “Data Swamps”. This can be a tough situation to revert especially if the data volume and velocity continue to increase. Avoiding this dilemma is absolutely critical for achieving data-driven value and providing customer satisfaction to users who are dependent on having reliable fast data retrieval to perform their downstream analytics job duties for their stakeholders.

Strategically, integrating and unifying a Data Warehouse and Data Lake becomes a situation where you need the best of both worlds to flexibly and elastically build a cost-efficient resilient enterprise ecosystem that seamlessly supports business intelligence & reporting, data science, and data engineering, machine learning, and artificial intelligence, and delivery of “Big Data” 5 V’s (Volume, Variety, Velocity, Veracity, Value). This is the idea and vision behind Lake House as a new unified data architecture that stitches the best components of Data Lakes and Data Warehouses together as one.

Databricks is the industry leader and original creator of Lakehouse architecture (i.e. Delta Lake). Amazon Web Services (AWS) is another pioneer with a Lake House architecture (i.e. Lake Formation + AWS Analytics). Some of the main high level technical features and solutions of a Lake House architecture include: ACID transactions, upserts [update + insert] & deletes, schema enforcement, file compaction, batch & streaming unification, and incremental loading. The 3 main open Data Lake table formats are Delta Lake, Apache Hudi, and Apache Iceberg. All three provide similar techniques to the features mentioned. In this blog I will discuss the fundamentals, building blocks, and solutions architecture of Databricks Lakehouse and AWS Lake House.

# Databricks Lakehouse
*Fundamentals*

Databricks Lakehouse is centered around a technology named Delta Lake, an open source project managed by the Linux Foundation. Delta Lake is a storage layer via Apache Parquet format that provides ACID-compliant transactions and additional benefits to Data Lakes. Databricks mentions 9 common Data Lake challenges Delta Lake can help address. They are:

* Hard to append data
* Jobs fail mid-way
* Modifications of existing data is difficult
* Real-time operations
* Costly to keep historical versions of data
* Difficult to handle large metadata
* “Too many files” problems
* Hard to get great performance
* Data quality issues

A Lakehouse architecture and the internals of Delta Lake are designed to eliminate the need to have always have a Data Warehouse/Data Lake two-tier architecture setup. With ACID transactions in a Data Lake the underlying data files linked to an external table will not be updated until a transactions either successfully completes or fails entirely. Thus, this simplifies incremental loading (i.e. appending data), avoiding jobs failing in process (i.e. ETL recovery), and modifying data (i.e deletes/updates/inserts). As a result, metadata discrepancies are significantly reduced because of the ACID-compliance and version control logging Delta Lake brings to Data Lakes.

In addition, the ability to combine (i.e. ETL synchronization) batch and streaming data pipelines provide more consistency and compliance for real-time operations and historical rollbacks [time travel]. Therefore, date staleness is less frequent and historical table versions are retained because Delta Lake always creates a new table version log available in real-time for each micro-batch transaction. One of the most apparent challenges with Data Lakes is achieving consistent great performance. A common culprit are small amounts of data split across [millions of] too many files (i.e. KB sized files). Realistically, tuning ETL jobs can even be difficult for the most technologically advanced specialists. Hence, Delta Lake can perform file compaction commands to optimize data layout & partitioning for faster query results and data quality checks.

*Building Blocks*

Databricks Lakehouse powered by Delta Lake contains some key internals designed to ensure data reliability and consistency. They are:

* Delta tables
* Delta files
* Delta transaction log
* Delta engine
* Delta storage layer

Delta tables are registered in a metastore (i.e. Apache Hive, AWS Glue) and contain the data’s underlying file location path, table properties, and schema definition. The delta files are stored in cloud object storage (i.e. AWS, MS Azure, GCP) or a file system (i.e. HDFS) as plain data files (i.e. Apache Parquet) or partitioned folders (i.e. Year-Month-Day) depending on the business logic. The delta transaction log is a very important folder named _delta_log. It essentially is the nucleus and key behind understanding Delta Lake because it tracks [in order] every transaction executed. It is a single source of truth and centralized repository for delta table changes. For example, each atomic commit on a delta table creates a new JSON file and CRC file containing various table metadata and statistics. This mechanism provides ACID transactions via atomicity where operations (i.e. INSERT, UPDATE) will not be performed in the Data Lake unless they are recorded and fully executed in the delta transaction log. Visually, here is a high level file system breakdown of Delta Lake’s directory structure under the hood.


Delta lake is bundled with a delta engine and delta storage layer, which are internals integrated via Apache Spark and cloud object storage. The engine provides dynamic file management pruning, delta caching, and many performance optimization benefits. The storage layer tracks metadata, schema changes, table version control, and many ACID-compliance operations.
