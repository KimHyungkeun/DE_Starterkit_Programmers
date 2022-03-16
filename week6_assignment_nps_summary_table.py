from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.models import Variable
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

# RedShift 연결
def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


# 실제 sql이 작동하는 함수
def execSQL(**context):

    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = """DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """.format(
	schema=schema,
	table=table)
    sql += select_sql
    cur.execute(sql)

    cur.execute("SELECT COUNT(1) FROM {schema}.temp_{table}""".format(schema=schema, table=table))
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError("{schema}.{table} didn't have any record".format(schema=schema, table=table))

    try:
        sql = """DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};""".format(
            schema=schema,
	    table=table)
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id = 'assignment_nps_summary',
    start_date = datetime(2022,3,16), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "hyungkeun_kim95"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table

# s3_key가 존재하지 않으면 에러를 냄! 
s3_folder_cleanup = S3DeleteObjectsOperator(
    task_id = 's3_folder_cleanup',
    bucket = s3_bucket,
    keys = s3_key,
    aws_conn_id = "aws_conn_id",
    dag = dag
)

mysql_to_s3_nps = MySQLToS3Operator(
    task_id = 'mysql_to_s3_nps',
    query = "SELECT * FROM prod.nps",
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    mysql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'],
    truncate_table = True,
    redshift_conn_id = "redshift_dev_db",
    dag = dag
)

# 기존 MySQL_to_Redshift DAG로 부터 nps 테이블을 만든다
# 그 후, nps를 계산한 값들을 모아놓은 nps_summary 테이블을 작성한다. (nps : 해당 일자의 전체 평점 갯수중 10,9 점을 준 평점 비율에서 0~6점을 준 평점 비율을 뺀 값이다.)
execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'hyungkeun_kim95',
        'table': 'nps_summary',
        'sql' : """SELECT a.date, ROUND((CASE WHEN b.counts is null then 0 else CAST(b.counts AS DOUBLE PRECISION) end - CASE WHEN c.counts is null then 0 else CAST(c.counts AS DOUBLE PRECISION) end ) / a.counts, 2) as nps_score
FROM
(SELECT to_char(created_at, 'yyyy-mm-dd') as date, count(score) as counts
FROM hyungkeun_kim95.nps 
GROUP BY date) a
LEFT OUTER JOIN
(SELECT to_char(created_at, 'yyyy-mm-dd') as date, count(score) as counts
FROM hyungkeun_kim95.nps 
WHERE score = 9 OR score = 10
GROUP BY date) b
ON a.date = b.date 
LEFT OUTER JOIN
(SELECT to_char(created_at, 'yyyy-mm-dd') as date, count(score) as counts
FROM hyungkeun_kim95.nps 
WHERE score >= 0 AND score <= 6
GROUP BY date) c 
ON a.date = c.date;"""
    },
    provide_context = True,
    dag = dag
)

s3_folder_cleanup >> mysql_to_s3_nps >> s3_to_redshift_nps >> execsql
