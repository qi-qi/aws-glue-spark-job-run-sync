# aws-glue-spark-job-run-sync
## Run - AWS Glue Spark Job with Synchronous Job Status Polling
https://github.com/qi-qi/aws-glue-spark-job-run-sync/blob/master/glue-job-run.py
```python
import logging
import sys
import time
import boto3

job_name = "qiqi-test"  # Name of the glue job
timeout = time.time() + 24 * 3600  # 24 hour from now

logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

glue = boto3.client('glue', region_name='eu-west-1')
run_id = glue.start_job_run(JobName=job_name)['JobRunId']

while True:
    status = glue.get_job_run(JobName=job_name, RunId=run_id)
    state = status['JobRun']['JobRunState']
    if state == 'SUCCEEDED':
        logging.info(f'Job: {job_name}, State: {state}, RunId: {run_id}')
        break
    elif state in ['STARTING', 'RUNNING', 'STOPPING']:
        logging.info(f'Job: {job_name}, State: {state}, RunId: {run_id}')
        time.sleep(10)  # check status every 10 seconds
    elif time.time() > timeout:
        raise Exception("Timeout!")
    else:
        raise Exception(f"Failed Job: {job_name}, State: {state}, RunId: {run_id}, Error: {status['JobRun']['ErrorMessage']}")
```

## Output in Console
```
2020-02-13 00:32:23,635 - INFO - Found credentials in shared credentials file: ~/.aws/credentials
2020-02-13 00:32:24,183 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:32:34,297 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:32:44,431 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:32:54,542 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:33:04,659 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:33:14,767 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:33:24,870 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:33:35,001 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:33:45,099 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:33:55,201 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:34:05,339 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:34:15,451 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:34:25,557 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
2020-02-13 00:34:35,676 - INFO - Job: qiqi-test, State: SUCCEEDED, RunId: jr_fad0bf6b22689087f8b9c5ac238769905816c7618fa15f1ae06a9f0c7fd277dc
```

## Sample glue pyspark job
https://github.com/qi-qi/aws-glue-spark-job-run-sync/blob/master/pyspark-sample-job.py
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3a://qiqi/sample.csv")

df.groupBy("YEARMONTH", "LOCATION") \
    .count() \
    .orderBy("count", ascending=False) \
    .repartition(1) \
    .write \
    .format("csv") \
    .option("header","true") \
    .mode("Overwrite") \
    .save("s3a://qiqi/result")
```
## Input
sample.csv:
```
YEARMONTH,KEY,LOCATION
201904,2216,Stockholm
201903,2215,Stockholm
201903,2215,Stockholm
201902,2216,Stockholm
201902,2215,Stockholm
201910,2247,Stockholm
201906,2215,Beijing
201901,2215,Stockholm
201908,2215,Stockholm
201910,2202,Stockholm
201902,2215,Stockholm
201901,2216,Stockholm
201904,2194,Stockholm
201901,2216,Stockholm
201902,2215,Stockholm
201911,2247,Stockholm
201909,2202,London
201901,2214,London
201905,2216,Stockholm
201909,2215,Stockholm
201907,2215,London
201909,2247,London
201909,2194,London
201910,2216,Stockholm
201905,2215,Stockholm
201907,2215,Stockholm
201910,2247,Stockholm
201902,2216,Stockholm
201904,2215,Beijing
201902,2194,Stockholm
201902,2215,Stockholm
201905,2216,Stockholm
201909,2247,Stockholm
201910,2215,Beijing
201911,2194,Stockholm
201903,2215,Stockholm
201903,2215,Stockholm
201909,2216,Stockholm
201905,2215,Stockholm
201906,2215,Stockholm
201906,2215,Stockholm
201903,2216,Beijing
201901,2216,Stockholm
201906,2215,Stockholm
201907,2215,Stockholm
201909,2215,Stockholm
201911,2216,Stockholm
201910,2194,Stockholm
201901,2215,Stockholm
```

## Result
```
YEARMONTH,LOCATION,count
201902,Stockholm,7
201901,Stockholm,5
201910,Stockholm,5
201903,Stockholm,4
201909,Stockholm,4
201905,Stockholm,4
201911,Stockholm,3
201906,Stockholm,3
201909,London,3
201907,Stockholm,2
201904,Stockholm,2
201904,Beijing,1
201907,London,1
201903,Beijing,1
201901,London,1
201910,Beijing,1
201908,Stockholm,1
201906,Beijing,1
```
