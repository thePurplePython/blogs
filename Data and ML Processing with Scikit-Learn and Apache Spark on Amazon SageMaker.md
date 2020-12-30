***Data and ML Processing with Scikit-Learn and Apache Spark on Amazon SageMaker***

## Introduction

Data processing (i.e. cleaning, preparing, transforming or curating) is an essential step to developing resilient, reusable, and reliable Machine Learning (ML) pipelines.  ***Amazon Web Services*** provides specific services for performing many data and ML processing tasks.  ***Amazon SageMaker*** is a ML service designed to build, train, and deploy ML models across the entire ML lifecycle. ***Scikit-Learn***  a.k.a SKLearn is a Python ML library designed to perform a plethora of data science duties for statistics, feature engineering, supervised learning, and unsupervised learning. ***Apache Spark*** is a distributed computing big data analytics framework designed to transform, engineer, and process massive amounts of data (think terabytes and petabytes) across a cluster of machines.  Spark contains many data engineering and data science interfaces such as Spark SQL, Spark Structured Streaming, and Spark ML.  Both SKLearn and Spark are fully supported and integrated within the ***SageMaker Python SDK*** hence providing the ability to deploy SKLearn/Spark code via ***Amazon SageMaker Processing***.  SageMaker Processing is an internal platform feature specialized to run various types of data and ML transformations (i.e. pre-processing & post-processing).  For example, feature engineering, as well as, generating training, validation, and test sets for data validation and model evaluation. 

Here is official **Amazon SageMaker Processing Documentation** on all current available features (https://sagemaker.readthedocs.io/en/stable/amazon_sagemaker_processing.html, https://docs.aws.amazon.com/sagemaker/latest/dg/processing-job.html).

SageMaker fully supports deploying customized data processing jobs for ML pipelines via SageMaker Python SDK.  For building, SageMaker has pre-built SKLearn/Spark docker images (fully managed running on Amazon EC2).  With SKLearn scripts, the main *SageMaker Class* is ```sagemaker.sklearn.processing.SKLearnProcessor```.  With Spark scripts, the main *SageMaker Classes* are ```sagemaker.spark.processing.PySparkProcessor``` (Python) and ```sagemaker.spark.processing.SparkJarProcessor``` (Scala).

In this blog using SageMaker Processing I will walk you through examples of how to deploy a customized data processing and feature engineering script on Amazon SageMaker via 1.) *SKLearn*, 2.) *Spark Python*, and 3.) *Spark Scala*.

The public dataset used for these demonstrations is available here:

https://archive.ics.uci.edu/ml/datasets/abalone

Each script will perform some basic feature engineering techniques.  First, as a best practice, the dataset will be split into train and test sets for model evaluation and preventative feature extraction leakage.  For categorical predictor variables, *One Hot Encoding*, will be implemented.  For numeric predictor variables, *Standard Scaler*, will be implemented.  It is important to note to perform both a ```.fit()``` and ```.transform()``` on the train set however only a ```.transform()``` on the test set.  This ensures the trained model will not include any bias, as well as, avoids learning/computing a new mean/variance on the unseen features in the test set.  This is a critical step to implement properly as each slice of the data serves a particular purpose in the pipeline.  Specifically, *Train* set (used to train model), *Validation* set (used to tune, estimate performance, and compare multiple models), and *Test* set (evaluate predictive strength of model).

***Disclaimer: The public datasets and EC2 instance types used in this blog contain very small data volumes and compute sizes.  Therefore, they are being used for demonstration purposes and cost savings only.  Size, configure, and tune infrastructure & applications accordingly.***

## Example 1: SKLearn SageMaker Processing

1a.) First, import dependencies and optionally set S3 bucket/prefixes if desired.

```python
import os
import sagemaker
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput

bucket = 'sagemaker-processing-examples'
code_prefix = 'scripts'
input_prefix = 'raw-datasets'
output_prefix = 'sklearn-datasets'
```

1b.) Next, initialize the appropriate class instance (i.e. ```SKLearnProcessor``` ) with any additional parameters.

```python
sklearn_job = SKLearnProcessor(
    framework_version='0.23-1',
    role=sagemaker.get_execution_role(),
    instance_type='ml.m5.xlarge',
    instance_count=1, # single machine computing
    base_job_name='sklearn-sagemaker-processing-example'
)
```

1c.) Now, execute the job with appropriate input(s), output(s), and argument(s).  For example, this job reads raw data stored in S3, splits data into train and test sets, performs feature engineering, and writes to storage (copied from internal EC2 local EBS volume to external S3).

```python
sklearn_job.run(code='s3://' + os.path.join(bucket, code_prefix, 'sklearn-processing.py'),
                inputs=[ProcessingInput(
                    input_name='raw',
                    source='s3://' + os.path.join(bucket, input_prefix, 'abalone.csv'),
                    destination='/opt/ml/processing/input')],
                outputs=[ProcessingOutput(output_name='train-dataset',
                                          source='/opt/ml/processing/output/train',
                                          destination='s3://' + os.path.join(bucket, output_prefix, 'train')),
                         ProcessingOutput(output_name='test-dataset',
                                          source='/opt/ml/processing/output/test',
                                          destination='s3://' + os.path.join(bucket, output_prefix, 'test'))],
                arguments=['--train-split-size', '0.80', '--test-split-size', '0.20']
               )
```

1d.) Confirm and view the S3 output results (features, labels) via AWS CLI and S3 Select Query.

```aws s3 --recursive ls s3://sagemaker-processing-examples/sklearn-datasets/```

![1d-sklearn-s3-output-paths.png](../master/images/1d-sklearn-s3-output-paths.png)

```SELECT * FROM s3object s LIMIT 5```

![1d-sklearn-features.png](../master/images/1d-sklearn-features.png)

```SELECT * FROM s3object s LIMIT 5```

![1d-sklearn-labels.png](../master/images/1d-sklearn-labels.png)

1e.)  For reference, here is the complete script (```sklearn-processing.py```) I developed that is being called by SageMaker in this example.

```python
# mods
import os
import argparse
import pandas as pd
import numpy as np
import sklearn as sk
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder

# schema
features = [
    'sex',
    'length',
    'diameter',
    'height',
    'whole_weight',
    'shucked_weight',
    'viscera_weight',
    'shell_weight']
labels = 'rings'
features_types = {
    'sex': 'category',
    'length': 'float64',
    'diameter': 'float64',
    'height': 'float64',
    'whole_weight': 'float64',
    'shucked_weight': 'float64',
    'viscera_weight': 'float64',
    'shell_weight': 'float64'}
labels_type = {'rings': 'float64'}

def main():
    
    # args
    parser = argparse.ArgumentParser()
    parser.add_argument('--train-split-size', type=float)
    parser.add_argument('--test-split-size', type=float)
    args = parser.parse_args()
    
    # read dataset
    df = pd.read_csv((os.path.join('/opt/ml/processing/input', 'abalone.csv')),
                     header=None,
                     names=features+[labels],
                     dtype={**features_types, **labels_type})
    
    # split dataset
    X_train, X_test, y_train, y_test = train_test_split(df.drop('rings', axis=1),
                                                    df['rings'],
                                                    train_size=args.train_split_size,
                                                    test_size=args.test_split_size,
                                                    shuffle=True,
                                                    random_state=0)
    
    # pipeline
    cat_features = df.select_dtypes(include='category').columns.tolist()
    num_features = df.select_dtypes(include='float64').columns.drop('rings').tolist()
    cat_transformer = Pipeline(steps=[
        ('cat_ohe', OneHotEncoder(handle_unknown='ignore'))])
    num_transformer = Pipeline(steps=[
        ('num_scaler', StandardScaler())])
    feature_eng_pipeline = ColumnTransformer(transformers=[
        ('cat', cat_transformer, cat_features),
        ('num', num_transformer, num_features)])    
    
    # fit model
    train_features = feature_eng_pipeline.fit_transform(X_train)
    test_features = feature_eng_pipeline.transform(X_test)
    
    # write
    pd.DataFrame(train_features).to_csv((os.path.join('/opt/ml/processing/output/train', 'train_features.csv')), header=False, index=False)
    pd.DataFrame(y_train).to_csv((os.path.join('/opt/ml/processing/output/train', 'train_labels.csv')), header=False, index=False)
    pd.DataFrame(test_features).to_csv((os.path.join('/opt/ml/processing/output/test', 'test_features.csv')), header=False, index=False)
    pd.DataFrame(y_test).to_csv((os.path.join('/opt/ml/processing/output/test', 'test_labels.csv')), header=False, index=False)

if __name__ == '__main__':
    main()
```

## Example 2: Spark Python SageMaker Processing

2a.) First, import dependencies and optionally set S3 bucket/prefixes if desired.

```python
import os
import sagemaker
from sagemaker.spark.processing import PySparkProcessor

bucket = 'sagemaker-processing-examples'
code_prefix = 'scripts'
logs_prefix = 'logs'
input_prefix = 'raw-datasets'
output_prefix = 'spark-python-datasets'
```

2b.) Next, initialize the appropriate class instance (i.e. ```PySparkProcessor``` ) with any additional parameters.

```python
spark_python_job = PySparkProcessor(
    framework_version='2.4',
    role=sagemaker.get_execution_role(),
    instance_type='ml.m5.xlarge',
    instance_count=2, # multiple machine distributed computing
    base_job_name='spark-python-sagemaker-processing-example'
)
```

2c.) Now, execute the job with appropriate argument(s).  For example, this job reads raw data stored in S3, splits data into train and test sets, performs feature engineering, and writes to S3 storage.

```python
spark_python_job.run(
    submit_app='s3://' + os.path.join(bucket, code_prefix, 'spark-python-processing.py'),
    arguments=['--s3-input-bucket', bucket,
               '--s3-input-prefix', input_prefix,
               '--train-split-size', '0.80',
               '--test-split-size', '0.20',
               '--s3-output-bucket', bucket,
               '--s3-output-prefix', output_prefix,
               '--repartition-num', '1'],
    spark_event_logs_s3_uri='s3://' + os.path.join(bucket, logs_prefix, 'spark-app-logs'),
    logs=False
)
```

2d.) Confirm and view the S3 output results (features + label dataframes) via AWS CLI and S3 Select Query.

```aws s3 --recursive ls s3://sagemaker-processing-examples/spark-python-datasets/```

![2d-spark-python-s3-output-paths.png](../master/images/2d-spark-python-s3-output-paths.png)

```SELECT * FROM s3object s LIMIT 1```

![2d-spark-python-feature-vector-label.png](../master/images/2d-spark-python-feature-vector-label.png)

2e.)  For reference, here is the complete script (```spark-python-processing.py```) I developed that is being called by SageMaker in this example.

```python
# mods
import os
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, StandardScaler, VectorAssembler

# create app
spark = SparkSession.builder.appName('spark-python-sagemaker-processing').getOrCreate()

# schema
schema = StructType([StructField('sex', StringType(), True), 
                     StructField('length', DoubleType(), True),
                     StructField('diameter', DoubleType(), True),
                     StructField('height', DoubleType(), True),
                     StructField('whole_weight', DoubleType(), True),
                     StructField('shucked_weight', DoubleType(), True),
                     StructField('viscera_weight', DoubleType(), True), 
                     StructField('shell_weight', DoubleType(), True), 
                     StructField('rings', DoubleType(), True)])

def main():
    
    #args
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3-input-bucket', type=str)
    parser.add_argument('--s3-input-prefix', type=str)
    parser.add_argument('--train-split-size', type=float)
    parser.add_argument('--test-split-size', type=float)
    parser.add_argument('--s3-output-bucket', type=str)
    parser.add_argument('--s3-output-prefix', type=str)
    parser.add_argument('--repartition-num', type=int)
    args = parser.parse_args()
    
    # read dataset
    df = spark.read.csv(('s3://' + os.path.join(args.s3_input_bucket, args.s3_input_prefix, 'abalone.csv')),
                        header=False,
                        schema=schema)
    
    # split dataset
    (train_df, test_df) = df.randomSplit([args.train_split_size, args.test_split_size], seed=0)
    
    # pipeline
    inx = StringIndexer(inputCol='sex', outputCol='sex_index')
    ohe = OneHotEncoder(inputCol='sex_index', outputCol='sex_ohe')
    va1 = VectorAssembler(inputCols=['length',
                                     'diameter',
                                     'height',
                                     'whole_weight',
                                     'shucked_weight',
                                     'viscera_weight',
                                     'shell_weight'],
                          outputCol='concats')
    ssr = StandardScaler(inputCol='concats', outputCol='scales')
    va2 = VectorAssembler(inputCols=['sex_ohe', 'scales'], outputCol='features')
    pl = Pipeline(stages=[inx, ohe, va1, ssr, va2])
    
    # fit model
    feature_eng_model = pl.fit(train_df)
    train_features_df = feature_eng_model.transform(train_df).select('features', 'rings')
    test_features_df = feature_eng_model.transform(test_df).select('features', 'rings')
    
    # write
    (train_features_df
    .repartition(args.repartition_num)
    .write
    .mode('overwrite')
    .parquet(('s3://' + os.path.join(args.s3_output_bucket, args.s3_output_prefix, 'train', 'train.parquet'))))
    (test_features_df
    .repartition(args.repartition_num)
    .write
    .mode('overwrite')
    .parquet(('s3://' + os.path.join(args.s3_output_bucket, args.s3_output_prefix, 'test', 'test.parquet'))))
    
    # kill app
    spark.stop()

if __name__ == '__main__':
    main()
```

## Example 3: Spark Scala SageMaker Processing

3a.) First, import dependencies and optionally set S3 bucket/prefixes if desired.

```python
import os
import sagemaker
from sagemaker.spark.processing import SparkJarProcessor

bucket = 'sagemaker-processing-examples'
code_prefix = 'scripts'
logs_prefix = 'logs'
input_prefix = 'raw-datasets'
output_prefix = 'spark-scala-datasets'
```

3b.) Next, initialize the appropriate class instance (i.e. ```SparkJarProcessor``` ) with any additional parameters.

```python
spark_scala_job = SparkJarProcessor(
    framework_version='2.4',
    role=sagemaker.get_execution_role(),
    instance_type='ml.m5.xlarge',
    instance_count=2, # multiple machine distributed computing
    base_job_name='spark-scala-sagemaker-processing-example'
)
```

3c.) Now, execute the job with appropriate argument(s).  For example, this job reads raw data stored in S3, splits data into train and test sets, performs feature engineering, and writes to S3 storage.  ***Please note the Spark Scala application jar file was compiled via SBT build tool***.

```python
spark_scala_job.run(
    submit_app='s3://' + os.path.join(bucket, code_prefix, 'spark-scala-processing_2.11-1.0.jar'),
    submit_class='sparkScalaSageMakerProcessing',
    arguments=[bucket,
               input_prefix,
               '0.80',
               '0.20',
               bucket,
               output_prefix,
               '1'],
    spark_event_logs_s3_uri='s3://' + os.path.join(bucket, logs_prefix, 'spark-app-logs'),
    logs=False
)
```

3d.) Confirm and view the S3 output results (features + label dataframes) via AWS CLI and S3 Select Query.

```aws s3 --recursive ls s3://sagemaker-processing-examples/spark-scala-datasets/```

![3d-spark-scala-s3-output-paths.png](../master/images/3d-spark-scala-s3-output-paths.png)

```SELECT * FROM s3object s LIMIT 1```

![3d-spark-scala-feature-vector-label.png](../master/images/3d-spark-scala-feature-vector-label.png)

3e.)  For reference, here is the complete script (```spark-scala-processing.scala```) I developed that is being called by SageMaker in this example.

```scala
// mods
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, StandardScaler, VectorAssembler}

object sparkScalaSageMakerProcessing {
    
    def main(args: Array[String]) {
        
        // args
        val s3InputBucket = args(0)
        val s3InputPrefix = args(1)
        val trainSplitSize = args(2).toFloat
        val testSplitSize = args(3).toFloat
        val s3OutputBucket = args(4)
        val s3OutputPrefix = args(5)
        val repartitionNum = args(6).toInt
        
        // create app
        val spark = SparkSession.builder().appName("spark-scala-sagemaker-processing").getOrCreate()
        
        import spark.implicits._

        // schema
        val schema = (new StructType()
                      .add("sex", StringType, true)
                      .add("length", DoubleType, true)
                      .add("diameter", DoubleType, true)
                      .add("height", DoubleType, true)
                      .add("whole_weight", DoubleType, true)
                      .add("shucked_weight", DoubleType, true)
                      .add("viscera_weight", DoubleType, true)
                      .add("shell_weight", DoubleType, true)
                      .add("rings", DoubleType, true)
                     )
        
        // read dataset
        val df = (spark
                  .read
                  .format("csv")
                  .option("header", "false")
                  .schema(schema)
                  .load("s3://" + s3InputBucket + "/" + s3InputPrefix + "/" + "abalone.csv"))
        
        // split dataset
        val Array(trainDf, testDf) = df.randomSplit(Array[Double](trainSplitSize, testSplitSize), seed=0)
        
        // pipeline
        val inx = (new StringIndexer().setInputCol("sex").setOutputCol("sex_index"))
        val ohe = (new OneHotEncoder().setInputCol("sex_index").setOutputCol("sex_ohe"))
        val va1 = (new VectorAssembler().setInputCols(Array("length",
                                                            "diameter",
                                                            "height",
                                                            "whole_weight",
                                                            "shucked_weight",
                                                            "viscera_weight",
                                                            "shell_weight"))
                   .setOutputCol("concats"))
        val ssr = (new StandardScaler().setInputCol("concats").setOutputCol("scales"))
        val va2 = (new VectorAssembler().setInputCols(Array("sex_ohe", "scales")).setOutputCol("features"))
        val pl = (new Pipeline().setStages(Array(inx, ohe, va1, ssr, va2)))        
        
        // fit model
        val featureEngModel = pl.fit(trainDf)
        val trainFeaturesDf = featureEngModel.transform(trainDf).select("features", "rings")
        val testFeaturesDf = featureEngModel.transform(testDf).select("features", "rings")
        
        // write
        (trainFeaturesDf
         .repartition(repartitionNum)
         .write
         .format("parquet")
         .mode("overwrite")
         .save("s3://" + s3OutputBucket + "/" + s3OutputPrefix + "/" + "train" + "/" + "train.parquet"))
        (testFeaturesDf
         .repartition(repartitionNum)
         .write
         .format("parquet")
         .mode("overwrite")
         .save("s3://" + s3OutputBucket + "/" + s3OutputPrefix + "/" + "test" + "/" + "test.parquet"))
   
        // kill app
        spark.stop()
    }
}
```

## Conclusion

This blog covers the essentials of getting started with SageMaker Processing via SKLearn and Spark.  SageMaker Processing also supports customized Spark tuning and configuration settings (i.e. ```spark.executor.cores```, ```spark.executor.memory```, etc.) via the ```configuration``` parameter that accepts a ```list[dict]``` or ```dict``` passed during the ```run()``` command.  Here is the official **Amazon SageMaker Processing Class Documentation** (https://sagemaker.readthedocs.io/en/stable/amazon_sagemaker_processing.html#learn-more).

AWS recently announced at re:Invent 2020 a few new SageMaker data preparation features including *Data Wrangler*, *Feature Store*, and *Clarify*.  Here is the official **Amazon SageMaker Features Documentation** (https://aws.amazon.com/sagemaker/features/).  Thank you for reading this blog.
