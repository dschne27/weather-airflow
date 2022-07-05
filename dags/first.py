# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# This makes scheduling easy
from airflow.utils.dates import days_ago
from airflow.models.xcom import XCom

import requests 
from datetime import datetime
import os


path_parent = os.path.dirname(os.getcwd())
S3_BUCKET = "dschne-bucket"
WEATHER_KEY = "docs/weatherlog.txt"

s3 = S3Hook(aws_conn_id='my_s3_conn')


def pushWeather(ti):
    fetched_key = ti.xcom_pull(key='todays_key', task_ids='write')
    fetched_file = ti.xcom_pull(key='todays_report', task_ids='write')
    s3.load_file(fetched_file, fetched_key, bucket_name=S3_BUCKET, replace=True)

# def deleteLog(path, key, bucket):
#     s3.delete_objects(bucket, key)


def get_conditions(ti):

    lat = 42.5426944
    lon = -83.3257472
    KEY = ""
    URL = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}".format(lat,lon, KEY)
    r = requests.get(URL)
    data = r.json()
    now = datetime.now()
    sky_cond = data['weather'][0]['main']
    temp = round((data['main']['temp'] - 273.15) * (9/5) + 32, 2)
    wind = data['wind']['speed']
    status = "{} {}F {}mph {}".format(sky_cond, temp, wind, now)
    fname = datetime.today().strftime('%Y-%m-%d') + ".txt"
    s3_key = "docs/" + fname

    f = open("/documents/report-" + fname, 'w')
    f.write(status + "\n")
    f.close()

    ti.xcom_push(key="todays_report", value=fname)
    ti.xcom_push(key="todays_key", value=s3_key)

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Daniel Schneider',
    'start_date': datetime(2022, 1, 1),
    'email': ['dschne@umich.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# defining the DAG

# define the DAG
dag = DAG(
    'first',
    default_args=default_args,
    description='My first DAG',
    schedule_interval='0 0 * * *',
)

# define the tasks
create_command = """
REPORT="report-$(date +%F)";
touch "documents/$REPORT.txt";
chmod 700 documents/$REPORT.txt;"""

# define the first task

file_report = BashOperator(
    task_id = 'file_report',
    bash_command=create_command,
    dag=dag
)

# define the second task
write = PythonOperator(
    task_id='write',
    python_callable=get_conditions,
    dag=dag
)


# define the third task
load = PythonOperator(
    task_id='load',
    python_callable=pushWeather,
    dag=dag
)

load.set_upstream(write)
write.set_upstream(file_report)