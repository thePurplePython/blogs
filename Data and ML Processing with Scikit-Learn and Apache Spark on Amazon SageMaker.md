***Data and ML Processing with Scikit-Learn and Apache Spark on Amazon SageMaker***

## Introduction

Data processing (i.e. cleaning, preparing, transforming or curating) is an essential step to developing resilient, reusable, and reliable Machine Learning (ML) pipelines.  ***Amazon Web Services*** provides specific services for performing many data and ML processing tasks.  ***Amazon SageMaker*** is a ML service designed to build, train, and deploy ML models across the entire ML lifecycle. ***Scikit-Learn***  a.k.a SKLearn is a Python ML library designed to perform a plethora of data science duties for statistics, feature engineering, supervised learning, and unsupervised learning. ***Apache Spark*** is a distributed computing big data analytics framework designed to transform, engineer, and process massive amounts of data (think terabytes and petabytes) across a cluster of machines.  Spark contains many data engineering and data science interfaces such as Spark SQL, Spark Structured Streaming, and Spark ML.  Both SKLearn and Spark are fully supported and integrated within the ***SageMaker Python SDK*** hence providing the ability to deploy SKLearn/Spark code via ***Amazon SageMaker Processing***.  SageMaker Processing is an internal platform feature specialized to run various types of data and ML transformations (i.e. pre-processing & post-porcessing).  For example, feature engineering, as well as, generating training, validation, and test sets for data validation and model evaluation. 

Here is official **Amazon SageMaker Processing Documentation** on all current available features (https://sagemaker.readthedocs.io/en/stable/amazon_sagemaker_processing.html, https://docs.aws.amazon.com/sagemaker/latest/dg/processing-job.html).

SageMaker fully supports deploying customized data processing jobs for ML pipelines via **SageMaker Python SDK**.  For building, SageMaker has pre-built SKLearn/Spark docker images (fully managed running on ***Amazon EC2***).  With SKLearn scripts, the main *SageMaker Class* is ```sagemaker.sklearn.processing.SKLearnProcessor```.  With Spark scripts, the main *SageMaker Classes* are ```sagemaker.spark.processing.PySparkProcessor``` (Python) and ```sagemaker.spark.processing.SparkJarProcessor``` (Scala).

In this blog using SageMaker Processing I will walk you through examples of how to deploy a customized data processing and feature engineering script on Amazon SageMaker via 1.) *SKLearn*, 2.) *Spark Python*, and 3.) *Spark Scala*.

The public dataset used for these demonstrations is available here: https://archive.ics.uci.edu/ml/datasets/abalone

Each script will perform some basic feature engineering techniques.  First, as a best practice, the dataset will be split into train and test sets for model evaluation and preventative feature extraction leakage.  For categorical predictor variables, *One Hot Encoding*, will be implemented.  For numeric predictor variables, *Standard Scaler*, will be implemented.  It is important to note to perform both a ```.fit()``` and ```.transform()``` on the train set however only a ```.transform()``` on the test set.  This ensures the trained model will not include any bias, as well as, avoids learning/computing a new mean/variance on the unseen features in the test set.  This is a critical step to implement properly as each slice of the data serves a particular purpose in the pipeline.  Specifically, *Train* set (used to train model), *Validation* set (used to tune, estimate performance, and compare multiple models), and *Test* set (evaluate predictive strength of model).

**Disclaimer: The public datasets and EC2 instance types used in this blog contain very small data volumes and compute sizes.  Therefore, they are being used for demonstration purposes and cost savings only.  Size, configure, and tune infrastructure & applications accordingly.**

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

## Example 2: Spark Python SageMaker Processing

## Example 3: Spark Scala SageMaker Processing
