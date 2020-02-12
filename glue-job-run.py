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
