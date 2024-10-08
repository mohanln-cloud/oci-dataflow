## Steps to read/write to AWS S3 object storage from OCI Data Flow

Data Flow Spark 3.2.1, Python, Scala Applications. 

Connecting with AWS provided standard sdk, Here are the steps.

* Create a python application in Data Flow with below spark configuration. This will pull the AWS dependency libraries to the job at the runtime (no need provide dependency jar files in the archive). make sure the application is open to internet to download the dependency, else attach the archive file packaging the dependency refer [third party doc](https://github.com/lnmohankumar/oci-dataflow/blob/main/quick-start).

```
  spark.jars.packages : org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-s3:1.11.655,com.amazonaws:aws-java-sdk-s3:1.11.655,com.amazonaws:aws-java-sdk-core:1.11.655
  spark.hadoop.fs.s3a.impl : org.apache.hadoop.fs.s3a.S3AFileSystem
  spark.hadoop.fs.s3a.access.key : <your AWS access key with s3 read/write permission>
  spark.hadoop.fs.s3a.secret.key : <your AWS secret key with s3 read/write permission>
```
* Python sample application code. In the sample application a csv file read from AWS s3 location, processed in Data Flow, written to oci object storage. Also, write back to S3 in the same spark job.

application arguments
```
s3a://mktestb1/samplecsv.csv oci://test@paasdevssspre/awsData/spark321 s3a://mktestb1/oci/deltaTabe
```
application code [AWSS3DataReader.py](https://github.com/lnmohankumar/oci-dataflow/blob/main/quick-start/oci-df-sample-py/AWSS3DataReader.py)


Note: The same configuration works with scala applications. 

