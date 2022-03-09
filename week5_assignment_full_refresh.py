from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


from datetime import datetime
from datetime import timedelta

import logging
import psycopg2
import requests

# 서울의 위도, 경도 => https://www.latlong.net/place/seoul-south-korea-621.html

# Redshift connection 함수
# Redshift 비밀번호는 Variable에 저장
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "hyungkeun_kim95"
    redshift_pass = Variable.get("redshift_pass")
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

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
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    del_sql = """BEGIN;DELETE FROM {schema}.{table};""".format(schema=schema, table=table)
    cur.execute(del_sql)

    n = len(lines)
    for i in range(7):
        if lines[i] != '' :
            (dt, day, min, max) = lines[i].split(",")
            logging.info(dt, "|", day, "|", min, "|", max)
            sql = """INSERT INTO {schema}.{table} VALUES ('{dt}', {day}, {min}, {max});""".format(schema=schema, table=table, dt=dt, day=day, min=min, max=max)
            logging.info(sql)
            cur.execute(sql)
    
    cur.execute("END;")

    
dag_full_refresh = DAG(
    dag_id = 'assignment_full_refresh',
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
    dag = dag_full_refresh)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    provide_context=True,
    dag = dag_full_refresh)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'hyungkeun_kim95',
        'table': 'weather_forecast_fullrefresh'
    },
    provide_context=True,
    dag = dag_full_refresh)

extract >> transform >> load