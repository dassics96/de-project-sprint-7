from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'dburkh',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='geo_user_friends_pipeline',
    default_args=default_args,
    description='Pipeline: user geo vitrina → zones vitrina → friends recommendations',
    schedule_interval='0 3 * * *',          # каждый день в 3:00 утра
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['spark', 'geo', 'recommendation'],
) as dag:

    #---------------------------------------------------------------
    #1. User geo vitrina (act_city, local_time и т.д.)
    #---------------------------------------------------------------
    user_geo_job = SparkSubmitOperator(
        task_id='user_geo_vitrina',
        name='User Geo Vitrina - {{ ds }}',
        application='/lessons/user_geo.py',
        conn_id='spark_default',                    # или твой spark connection в Airflow
        conf={
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '2g',
            # 'spark.dynamicAllocation.enabled': 'true',
        },
        application_args=[
            '/user/master/data/geo/events',          # source
            '/user/dburkh/data/geo/events',          # target partitioned events
            '/user/dburkh/analytics/user_geo',       # output vitrina
        ],
        verbose=True,
        dag=dag,
    )

    #---------------------------------------------------------------
    #2. Geo zones vitrina (недельные & мес аггрегации)
    #---------------------------------------------------------------
    zones_job = SparkSubmitOperator(
        task_id='geo_zones_vitrina',
        name='Geo Zones Vitrina - {{ ds }}',
        application='/lessons/geo_zones.py',
        conn_id='spark_default',
        conf={
            'spark.executor.memory': '6g',
            'spark.executor.cores': '3',
            'spark.driver.memory': '3g',
        },
        application_args=[
            '/user/dburkh/data/geo/events',          # уже переложенные события
            '/user/dburkh/data/geo/geo.csv',
            '/user/dburkh/analytics/geo_zones',
        ],
        verbose=True,
        dag=dag,
    )

    #---------------------------------------------------------------
    #3. Friends recommendation
    #---------------------------------------------------------------
    friends_rec_job = SparkSubmitOperator(
        task_id='friends_recommendation',
        name='Friends Recommendation - {{ ds }}',
        application='/lessons/friends_recommendation.py',
        conn_id='spark_default',
        conf={
            'spark.executor.memory': '5g',
            'spark.executor.cores': '3',
            'spark.driver.memory': '3g',
        },
        application_args=[
            '/user/dburkh/data/geo/events',
            '/user/dburkh/data/geo/geo.csv',
            '/user/dburkh/analytics/friends_rec',
        ],
        verbose=True,
        dag=dag,
    )

    #---------------------------------------------------------------
    #Flow: user_geo → zones → friends
    #---------------------------------------------------------------
    start = DummyOperator(task_id='start', dag=dag)
    end   = DummyOperator(task_id='end', dag=dag)

    start >> user_geo_job >> zones_job >> friends_rec_job >> end
