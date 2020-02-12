# aws-glue-spark-job-run-sync
## Run AWS Glue Spark Job with Synchronous Job Status Polling
```python
import logging
import sys
import time
import boto3

logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

job_name = "qiqi-test"
timeout = time.time() + 24 * 3600  # 24 hour from now

glue = boto3.client('glue', region_name='eu-west-1')
run_id = glue.start_job_run(JobName=job_name)['JobRunId']

while time.time() < timeout:
    status = glue.get_job_run(JobName=job_name, RunId=run_id)
    state = status['JobRun']['JobRunState']
    if state == 'SUCCEEDED':
        logging.info(f'Job: {job_name}, State: {state}, RunId: {run_id}')
        break
    elif state in ['STARTING', 'RUNNING', 'STOPPING']:
        logging.info(f'Job: {job_name}, State: {state}, RunId: {run_id}')
        time.sleep(10)  # check status every 10 seconds
    else:
        raise Exception(f"Failed Job: {job_name}, State: {state}, RunId: {run_id}, Error: {status['JobRun']['ErrorMessage']}")

raise Exception("Timeout!")
```

## Output in Console
```
2020-02-12 23:46:25,336 - INFO - Found credentials in shared credentials file: ~/.aws/credentials
2020-02-12 23:46:26,996 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_41c4ddafa440818d9223cd4238430a2696f09b5510100770feeeb4c148b79ac7
2020-02-12 23:46:37,116 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_41c4ddafa440818d9223cd4238430a2696f09b5510100770feeeb4c148b79ac7
...
2020-02-12 23:59:15,570 - INFO - Job: qiqi-test, State: RUNNING, RunId: jr_41c4ddafa440818d9223cd4238430a2696f09b5510100770feeeb4c148b79ac7
2020-02-12 23:59:25,676 - INFO - Job: qiqi-test, State: SUCCEEDED, RunId: jr_41c4ddafa440818d9223cd4238430a2696f09b5510100770feeeb4c148b79ac7
```

## Sample glue spark job
```python

```
## Sample input csv
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

## Sample output csv:
