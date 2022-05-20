"""
Airflow, Open Weather API를 활용하요
이후 7일간의 서울시 낮기온, 최고 기온, 최저 기온 데이터 Redshift에 적재
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging
import psycopg2
import json


def get_Redshift_connection():
    # Airflow 내 Connections 활용
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def extract(**context):
    api_key = context['params']['api_key']
    latitude = context['params']['latitude']
    longitude = context['params']['longitude']

    link = f'https://api.openweathermap.org/data/2.5/onecall?lat={latitude}&lon={latitude}&appid={api_key}&units=metric'
    
    f = requests.get(link)
    f_js = f.json()
  

    return json.dumps(f_js)


def transform(**context):
    my_json_text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    my_json = json.loads(my_json_text)
    daily = my_json.get('daily')
    date_arr = []
    day_arr = []
    max_arr = []
    min_arr = []

    
    for d in daily :
        dt = datetime.fromtimestamp(d.get('dt')).strftime('%Y-%m-%d')
        date_arr.append(dt)
        day_arr.append(d.get('temp').get('day'))
        max_arr.append(d.get('temp').get('max'))
        min_arr.append(d.get('temp').get('min'))
    
    all_arr = [date_arr, day_arr, max_arr, min_arr]
    return all_arr


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    arr = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    date_arr = arr[0]
    day_arr = arr[1]
    max_arr = arr[2]
    min_arr = arr[3]

    try:
        # 테이블 없을 경우 새롭게 생성
        sql = "CREATE TABLE IF NOT EXISTS {schema}.{table} ( date date primary key, temp float, min_temp float, max_temp float, created_date timestamp default GETDATE() );".format(schema=schema, table=table)
        sql += "DELETE FROM {schema}.{table};".format(schema=schema, table=table)
        for index, value in enumerate(date_arr):
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{date_arr[index]}', '{day_arr[index]}', '{min_arr[index]}', '{max_arr[index]}');"""
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")

    


dag_assignment = DAG(
    dag_id = 'open_weather_practice',
    start_date = datetime(2022,5,12),
    schedule_interval = '0 2 * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'api_key':  Variable.get("open_weather_api_key"),
        'latitude': Variable.get("latitude"),
        'longitude': Variable.get("longitude")
    },
    provide_context=True,
    dag = dag_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    provide_context=True,
    dag = dag_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'heffy127',
        'table': 'weather_forecast'
    },
    provide_context=True,
    dag = dag_assignment)

extract >> transform >> load
