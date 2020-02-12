# aws-glue-spark-job-run-sync
aws-glue-spark-job-run-sync
```
import boto3
import time

job_name = "qiqi-test"
timeout = time.time() + 24 * 3600  # 24 hour from now

glue = boto3.client('glue', region_name='eu-west-1')
run_id = glue.start_job_run(JobName=job_name)['JobRunId']

while time.time() < timeout:
    status = glue.get_job_run(JobName=job_name, RunId=run_id)
    state = status['JobRun']['JobRunState']
    if state == 'SUCCEEDED':
        print(f'Succeeded job: {job_name}, Run id: {run_id}')
        break
    elif state in ['STARTING', 'RUNNING', 'STOPPING']:
        print(f'Running Job: {job_name}, Run id: {run_id}, State: {state}')
        time.sleep(10)  # check status every 10 seconds
    else:
        raise Exception(
            f"Failed job: {job_name}, Run id: {run_id}, State: {state}, Error: {status['JobRun']['ErrorMessage']}")

raise Exception("Timeout!")
```
