from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


from datetime import datetime
from datetime import timedelta

import logging
import psycopg2
import requests

# 서울의 위도, 경도 => https://www.latlong.net/place/seoul-south-korea-621.html

# Redshift connection 함수
def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

# weather_api를 Call하여 JSON 형식으로 읽어들인다 
def extract(**context):
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)
    f_js = f.json()
    return f_js

# JSON내의 내용 중, 일주일간의 날씨정보를 불러온다 (dt:날짜, day:낮기온, min:최저기온, max:최고기온)
def transform(**context):
    json = context['task_instance'].xcom_pull(key="return_value", task_ids="extract")
    row = json['daily']
    ans = []
    for i in range(len(row)) :   
      line = datetime.fromtimestamp(row[i]['dt']).strftime('%Y-%m-%d')+","+str(row[i]['temp']['day'])+","+str(row[i]['temp']['min'])+","+str(row[i]['temp']['max'])
      ans.append(line)
    return ans

# transform에서 필터링한 내용을 이용해서, 일주일간의 날씨정보를 RedShift DB에 저장한다.
def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    tmp_table = "temp_weather_forecast"
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    # CTAS, CREATE TABLE LIKE을 해도 created_at의 default값을 반영하지 못하여 alter table 쿼리를 임시로 부여
    create_sql = """CREATE TABLE {schema}.{tmp} ( LIKE {schema}.{table});""".format(schema=schema, tmp = tmp_table, table=table)
    alter_sql = """ALTER TABLE {schema}.{tmp} DROP COLUMN created_date;
    ALTER TABLE {schema}.{tmp} ADD COLUMN created_date timestamp DEFAULT GETDATE();""".format(schema=schema, tmp = tmp_table)
    insert_tmp_sql = """INSERT INTO {schema}.{tmp} (SELECT * FROM {schema}.{table})""".format(schema=schema, tmp = tmp_table, table=table)

    cur.execute(create_sql)
    cur.execute(alter_sql)
    cur.execute(insert_tmp_sql)

    n = len(lines)
    for i in range(7):
        if lines[i] != '' :
            (dt, day, min, max) = lines[i].split(",")
            logging.info(dt, "|", day, "|", min, "|", max)
            sql = """INSERT INTO {schema}.{tmp} VALUES ('{dt}', {day}, {min}, {max});""".format(schema=schema, tmp = tmp_table, dt=dt, day=day, min=min, max=max)
            logging.info(sql)
            cur.execute(sql)
   
    del_sql = """BEGIN;DELETE FROM {schema}.{table};""".format(schema=schema, table=table)
    insert_sql = """INSERT INTO {schema}.{table} SELECT date, temp, min_temp, max_temp, created_date
                    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) 
                    seq FROM {schema}.{tmp}) WHERE seq = 1;""".format(schema=schema, tmp = tmp_table, table=table)
    
    
    cur.execute(del_sql)
    cur.execute(insert_sql)
    cur.execute("END;")

    drop_tmp_sql = """DROP TABLE {schema}.{tmp}""".format(schema=schema, tmp = tmp_table)
    cur.execute(drop_tmp_sql)





dag_incremental_update = DAG(
    dag_id = 'assignment_incremental_update',
    start_date = datetime(2022,3,9), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 1 * * *',  # 적당히 조절
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url': Variable.get("open_weather_api_key")
    },
    provide_context=True,
    dag = dag_incremental_update)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    provide_context=True,
    dag = dag_incremental_update)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'hyungkeun_kim95',
        'table': 'weather_forecast'
    },
    provide_context=True,
    dag = dag_incremental_update)

extract >> transform >> load