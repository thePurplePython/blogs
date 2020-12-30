***Data and ML Processing with Scikit-Learn and Apache Spark on Amazon SageMaker***

## Introduction

Data processing (i.e. cleaning, preparing, transforming or curating) is an essential step to developing resilient, reusable, and reliable Machine Learning (ML) pipelines.  ***Amazon Web Services*** provides specific services for performing many data and ML processing tasks.  ***Amazon SageMaker*** is a ML service designed to build, train, and deploy ML models across the entire ML lifecycle. ***Scikit-Learn***  a.k.a SKLearn is a Python ML library designed to perform a plethora of data science duties for statistics, feature engineering, supervised learning, and unsupervised learning. ***Apache Spark*** is a distributed computing big data analytics framework designed to transform, engineer, and process massive amounts of data (think terabytes and petabytes) across a cluster of machines.  Spark contains many data engineering and data science interfaces such as Spark SQL, Spark Structured Streaming, and Spark ML.  Both SKLearn and Spark are fully supported and integrated within the ***SageMaker Python SDK*** hence providing the ability to deploy SKLearn/Spark code via ***Amazon SageMaker Processing***.  SageMaker Processing is an internal platform feature specialized to run various types of data and ML transformations (i.e. pre-processing & post-porcessing).  For example, feature engineering, as well as, generating training, validation, and test sets for data validation and model evaluation. 

Here is official **Amazon SageMaker Processing Documentation** on all current available features (https://sagemaker.readthedocs.io/en/stable/amazon_sagemaker_processing.html, https://docs.aws.amazon.com/sagemaker/latest/dg/processing-job.html).

SageMaker fully supports deploying customized data processing jobs for ML pipelines via SageMaker Python SDK.  For building, SageMaker has pre-built SKLearn/Spark docker images (fully managed running on Amazon EC2).  With SKLearn scripts, the main *SageMaker Class* is ```sagemaker.sklearn.processing.SKLearnProcessor```.  With Spark scripts, the main *SageMaker Classes* are ```sagemaker.spark.processing.PySparkProcessor``` (Python) and ```sagemaker.spark.processing.SparkJarProcessor``` (Scala).

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

2a.) Next, initialize the appropriate class instance (i.e. ```SKLearnProcessor``` ) with any additional parameters.

```python
sklearn_job = SKLearnProcessor(
    framework_version='0.23-1',
    role=sagemaker.get_execution_role(),
    instance_type='ml.m5.xlarge',
    instance_count=1,
    base_job_name='sklearn-sagemaker-processing-example'
)
```

3a.) Now, execute the job with appropriate input(s), output(s), and argument(s).  For example, this job reads raw data stored in S3, splits data into train and test sets, performs feature engineering, and writes to storage (copied from internal EC2 local EBS volume to external S3).

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

4a.) Confirm and view the S3 output results (features, labels) via AWS CLI ```aws s3 --recursive ls s3://sagemaker-processing-examples/sklearn-datasets/``` and S3 Select Query ```SELECT * FROM s3object s LIMIT 5```.

![4a-sklearn-features.png](../master/images/4a-sklearn-features.png)

![4a-sklearn-labels.png](../master/images/4a-sklearn-labels.png)

5a.)  Here is the complete script I developed for reference.

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
    'sex': "category",
    'length': "float64",
    'diameter': "float64",
    'height': "float64",
    'whole_weight': "float64",
    'shucked_weight': "float64",
    'viscera_weight': "float64",
    'shell_weight': "float64"}
labels_type = {'rings': "float64"}

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
    num_features = df.select_dtypes(include='float64').columns.drop('rings').tolist()
    cat_features = df.select_dtypes(include='category').columns.tolist()
    num_transformer = Pipeline(steps=[
        ('num_imputer', SimpleImputer(strategy='median')),
        ('num_scaler', StandardScaler())])
    cat_transformer = Pipeline(steps=[
        ('cat_imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('cat_ohe', OneHotEncoder(handle_unknown='ignore'))])
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

if __name__ == "__main__":
    main()
```

## Example 2: Spark Python SageMaker Processing

## Example 3: Spark Scala SageMaker Processing
