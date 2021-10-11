***The Fundamentals of Data Warehouse + Data Lake = Lake House***

## Introduction
With the evolution of Data Warehouses and Data Lakes, they have certainly become more specialized yet siloed in their respective landscapes over the last few years. Both data management technologies each have their own identities and are best used for certain tasks and needs, however they also struggle in providing some important abilities. Data Warehouse advantages are focused around analyzing structured data, OLTP, schema-on-write, SQL, and delivering ACID-compliant database transactions. Data Lake advantages are focused around analyzing all types of data (structured, semi-structured, unstructured), OLAP, schema-on-read, API connectivity, and low-cost object storage systems for data in open file formats (i.e. Apache Parquet).

Notably, Data Warehouses particularly struggle with support for advanced data engineering, data science, and machine learning. For example, their inability to store unstructured data (i.e. text, images, video, feature engineering vectors, etc.) for machine learning development. In addition, proprietary Data Warehouse software are expensive and struggle with integrating open source + cloud platform data science and data engineering tools (i.e. Python, Scala, Spark, SageMaker, Anaconda, DataRobot, SAS, R, etc.) for exploratory data analysis via notebooks, distributed compute processing, hosting deployed models, and storing inference pipeline results. System integration, data movement costs, and data staleness will even become more challenging (especially with limited technology choices at your disposal) to address in a hybrid on-premise cloud environment.

On the flip side, unfortunately, Data Lakes sometimes notoriously struggle with data quality, transactional support, data governance, and query performance issues. Data Lakes built without vital skills, key capabilities, and specialized technologies will inevitably over time turn into “Data Swamps”. This can be a tough situation to revert especially if the data volume and velocity continue to increase. Avoiding this dilemma is absolutely critical for achieving data-driven value and providing customer satisfaction to users who are dependent on having reliable fast data retrieval to perform their downstream analytics job duties for their stakeholders.

Strategically, integrating and unifying a Data Warehouse and Data Lake becomes a situation where you need the best of both worlds to flexibly and elastically build a cost-efficient resilient enterprise ecosystem that seamlessly supports business intelligence & reporting, data science, and data engineering, machine learning, and artificial intelligence, and delivery of “Big Data” 5 V’s (Volume, Variety, Velocity, Veracity, Value). This is the idea and vision behind Lake House as a new unified data architecture that stitches the best components of Data Lakes and Data Warehouses together as one.

Databricks is the industry leader and original creator of Lakehouse architecture (i.e. Delta Lake). Amazon Web Services (AWS) is another pioneer with a Lake House architecture (i.e. Lake Formation + AWS Analytics). Some of the main high level technical features and solutions of a Lake House architecture include: ACID transactions, upserts [update + insert] & deletes, schema enforcement, file compaction, batch & streaming unification, and incremental loading. The 3 main open Data Lake table formats are Delta Lake, Apache Hudi, and Apache Iceberg. All three provide similar techniques to the features mentioned. In this blog I will discuss the fundamentals, building blocks, and solutions architecture of Databricks Lakehouse and AWS Lake House.

## Databricks Lakehouse
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

![1-delta-log.png](../master/images/1-delta-log.png)

Delta lake is bundled with a delta engine and delta storage layer, which are internals integrated via Apache Spark and cloud object storage. The engine provides dynamic file management pruning, delta caching, and many performance optimization benefits. The storage layer tracks metadata, schema changes, table version control, and many ACID-compliance operations.

*Solutions Architecture*

Databricks Lakehouse follows a design pattern architecture delivering multi-layers of data quality and curation via a 3 table tier nomenclature. They are:

* Bronze
* Silver
* Gold

These layers each serve an important purpose in the delta architecture pipeline built to ensure data is highly available for multiple downstream use cases. Bronze stores raw operational data (i.e. OLTP), typically involves little data processing, and serves as a system of record. This layer is often the landing zone where the Data Lake ingests raw data (i.e. JSON, CSV) via a combination of batch and streaming jobs.

Silver stores clean atomic data (i.e. OLAP), typically involves some data processing, and serves as a single source of truth with schema enforcement stressed. This layer may involve batch and streaming jobs converting JSON to Parquet or ORC, as well as, re-partitioning data per some business logic and performing file compaction techniques via Spark or Delta Lake to improve query performance. Gold stores aggregated departmental data ‘marts’ (i.e. OLAP), typically involves a lot of data processing, and serves as a presentation layer for business intelligence and data science queries. This layer provides filtered data ready for individual users to perform ad-hoc queries and consume OLAP cubes via BI tools, SQL, Python, Apache Spark, etc.

Sometimes the silver layer can be the main source for machine learning and feature engineering to build out a feature store, curated data repository, or an inference pipeline. It will most likely depend on the organization and use case(s) involved. Visually, here is a high level data pipeline build diagram for a Databricks Lakehouse augmented delta architecture.

![2-curated-layers.png](../master/images/2-curated-layers.png)

In summary, Databricks Lakehouse is leading and innovating the way for providing one platform environment that can do it all. It reduces the real-world challenges of moving and connecting data across siloed systems (i.e. analogy → traveling and communicating between separate planets in a solar system) in an enterprise. To consistently get started and deliver value to BI, AI/ML, Big Data Analytics, etc., I firmly believe the Lakehouse architecture will continue to evolve, educate, and empower organizations towards making better business decisions with data. Thus, ensuring challenges such as “Data Swamp”, dirty unethical data discrepancies, bad I/O performance, slow data movement, tool integration issues, and team silos don’t continue to hold you back from solving customer problems and delivering data-driven solutions!

For reference, here are the official white-papers for Lakehouse and Delta Lake to learn more from the experts.

https://drive.google.com/viewerng/viewer?url=https://cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf

https://drive.google.com/viewerng/viewer?url=https://databricks.com/wp-content/uploads/2020/08/p975-armbrust.pdf

## Amazon Web Services Lake House
*Fundamentals*

AWS Lake House is focused around using many of the AWS Analytics services in tandem. Specifically, integrating these specialized services to build seamless interaction between Data Lake, Data Warehouse, and the data movement between systems. AWS is a firm believer of using the right tool for the right job, which I personally advocate too. Often you see projects fail or implement bandaid solutions because the right tool was not available or selected. After all, chopping down a tree with a knife (or even an axe) is cumbersome and ludicrous when a chain saw is the best and recommended tool purposely designed for the particular task at hand. If shortcuts are taken and obsolete technologies are chosen, over time your Data Lake will crumble and succumb to suffering performance penalties and becoming a “Data Swamp”. Hence, AWS Lake House stresses how important it is to ensure data movement between AWS services become easier by design. Therefore, AWS describes the 3 data movement scenarios as:

* Inside-out
* Outside-in
* Around-the-perimeter

For example, inside-out refers to collecting data from an internal Data Lake and copying it over to another system (i.e. aggregating logs in Amazon Elasticsearch). Similarly, outside-in refers to the opposite direction of transferring data from an external Data Warehouse to an internal Data Lake or file system (i.e. performing feature engineering in Amazon S3 & Amazon SageMaker). Sometimes, external data stores replicate data around-the-perimeter to scale better performance (i.e. Amazon RDS to Amazon Redshift cluster migration) and or improve the user experience. There are many customer use cases for all 3 of these data movement scenarios.

AWS refers to data growth and data movement difficulty as “data gravity”, which is basically related to the “Big Data” phrase where data presence and increasing volumes occur across many IT systems. Resulting in the need for a data architecture capable of meeting customer demand, infrastructure management costs, data locality performance, and data & system integration. AWS documents their Well-Architected framework of 5 pillars as:

* Operational Excellence
* Security
* Reliability
* Performance Efficiency
* Cost Optimization

These pillars are the foundation to achieving a data architecture that provides continuous scalable performance, flexible service tool choices, data & system integration, secure data protection, and infrastructure elasticity. A Lake House architecture starts with adopting these fundamental best practices.

*Building Blocks*

AWS Lake House has 5 elements of data architecture. They are:

* Scalable data lakes
* Purpose-built data services
* Seamless data movement
* Unified governance
* Performant and cost-effective

Each element serves a purpose via specialized AWS services to deliver a resilient, scalable, elastic, secure, and flexible architecture. AWS Lake Formation is the staple for building secure and scalable Data Lakes via Amazon S3. Some new Lake Formation features include ACID transactions and governed tables to address faster query performance via file compaction methods. These are very similar features like Delta Lake discussed in the Databricks section of this blog post.

For specific data needs AWS brings many data services to your disposal: Amazon Athena for interactive SQL queries, Amazon EMR for Spark data processing, Amazon Elasticsearch for search analytics, Amazon Kinesis for real-time streaming, and Amazon Redshift for data warehousing. These services are essential for building advanced analytics workflows and having diverse tools that connect to the Lake House ecosystem.

Using and integrating these services will result in inside-out, outside-in, and around-the-perimeter data movement between source and target systems. Here shines AWS Glue a server-less data integration service specialized in ETL, table catalogs, schema inference, job scheduling, and most recently virtualizing tables as materialized views across databases and data stores via SQL. These server-less views, referred to as, Elastic Views, provide automated data replication, continuous monitoring, and up-to-date data refreshes making data movement and infrastructure easier to manage and virtualize.

Lake Formation also provides row-level security features for sharing data and controlling access that eliminate the need to copy data across data stores. This helps saves storage costs and additional infrastructure pipeline layers while maintaining data governance. Last but not least, AWS strives to provide top-tier performance and cost-savings via Amazon EC2 compute with multiple purchase options (i.e. On-Demand, Reserved, Spot, Savings) for all your use case needs. Additionally, more optimized instances types continue to be released too for Amazon EMR and Amazon Redshift. Overall, these 5 key elements spearhead the implementation of the AWS Lake House architecture.

*Solutions Architecture*

AWS Lake House follows an ecosystem architecture via 5 layers that address data gravity using specialized AWS services stationed on the periphery of a centralized Data Lake. These flexible layers are:

* Ingestion layer
* Storage layer
* Catalog layer
* Processing layer
* Consumption layer

The ingestion layer performs data migrations via batch and streaming methods. The main AWS offerings include Data Migration Service for RDBMS migrations and Amazon Kinesis (i.e. Data Firehose, Data Streams) for buffering data delivery to Amazon S3 or Amazon Redshift. The storage layer includes Amazon S3 as a Data Lake and Amazon Redshift as a Data Warehouse. Amazon S3 object store provides cheap storage and the ability to store diverse types of schemas in open file formats (i.e. Apache Parquet, Apache ORC, Apache Avro, CSV, JSON, etc.) as schema-on-read. Amazon Redshift provides storing data in tables as structured dimensional or denormalized schemas as schema-on-write.

The catalog layer involves metadata management and registration. Lake Formation and AWS Glue serve as the central catalog services to track metadata for all Lake House datasets. Capabilities include table versioning, schema & partitioning definitions, data location, table permissions, and securing business & data owner information. The processing layer builds ETL jobs into organized buckets or prefixes as landing, raw, trusted, and curated zones. This is on the same page as the bronze, silver, and gold nomenclature by Databricks Lakehouse. A plethora of data processing services are available to perform these tasks. Depending on the use case, AWS offerings include Amazon EMR (i.e. Apache Spark), Kinesis Data Analytics (i.e. Apache Flink), AWS Glue, AWS Lambda, Amazon SageMaker Processing, Amazon Redshift SQL (i.e. Redshift Spectrum), and single machine Amazon EC2 instances for smaller datasets.

The consumption layer provides many Lake House interfaces for analytics use cases like ad-hoc SQL querying, business intelligence, and machine learning. For SQL, AWS provides Amazon Athena and Redshift Spectrum. For business intelligence, you can create visualizations and dashboards via Amazon QuickSight. For end-to-end machine learning, AWS utilizes Amazon SageMaker to build, train, and deploy models and inference pipelines.

Visually, here is a high level layered ecosystem build diagram for an AWS Lake House architecture. Please note some AWS Analytics services are not included.

![3-aws-lake-house.png](../master/images/3-aws-lake-house.png)

Each of these 5 layers play their own part to enabling a seamless flow of data movement within a Lake House. The key is the technical flexibility available to iteratively scale and adjust accordingly by using the right service for the right job at an affordable (pay for what you use) price.
For reference, here are official AWS blogs for AWS Analytics & Lake House best practices design to learn more in depth details.

https://aws.amazon.com/blogs/big-data/harness-the-power-of-your-data-with-aws-analytics/

https://aws.amazon.com/blogs/big-data/build-a-lake-house-architecture-on-aws/

## Conclusion

Overall, both architectures provide very similar solutions. From a cultural perspective Databricks Lakehouse and AWS Lake House are revolutionizing better ways to manage, store, process, and consume data. From a technical perspective Databricks Lakehouse utilizes Delta Lake, Apache Parquet, and Apache Spark; AWS Lake House utilizes a plethora of AWS services leveraging a lot of the open source Apache projects mentioned throughout this blog. Some of the main technical focal points include:

1. Separation of compute and storage — elastic compute, scalable infrastructure, cheap storage, and highly available reusable data
2. Reliability & consistency — improved data quality, less corrupt & duplicate data, file size optimization & compaction, less schema/metadata mismatches, and guaranteed complete transactions & rollback options
3. Real-time streaming support — continuous/immediate incremental delta updates & changes as new data arrives while maintaining great query performance, joining of batch and streaming data across lines of business, removal of complex workflows (i.e. Lambda Architecture → parallel data pipelines of a streaming job that continuously appends + batch job that re-partitions, updates, and overwrites files per some cadence [daily])

It is exciting these data solutions can be solved and delivered when combining the readiness of a committed work culture with cutting-edge technical expertise and capabilities. I firmly believe Lakehouse/Lake House can help organizations and employees experience a more data-driven culture, up-skill/educate & increase talent growth, save cloud [or reduce on-premise vendor lock-in] costs, simplify existing & new architecture designs, and better unify teams working in organization silos on similar analytics efforts by solidifying data as a single source of truth. The sky is the limit!

Thank you for reading this blog post. I have been fascinated by the emergence of this innovative technology and look forward to continuing to contribute these techniques on technical implementations, education demonstrations, and consulting engagements. Happy learning, problem solving, and solution delivery!
