import datetime
import airflow
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator,DataProcPySparkOperator
from airflow.contrib.operators.dataflow_operator import  DataFlowPythonOperator
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Kamal',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

query_part = """
create external table if not exists customer_part
 (customerName string, phone string)  
 ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
 STORED AS TEXTFILE LOCATION 'gs://mysqldataflow/json/customer/'
"""

query_all = """
create external table if not exists customer_all
 (cust_details string)  
 STORED AS TEXTFILE LOCATION 'gs://mysqldataflow/json/customer/'
"""
dag=  airflow.DAG(
        'Import-MySQL-to-GS-and-DataProc',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1))

t1 = DataProcPySparkOperator(task_id='import-mysql-data', main='gs://mysqlnosql/spark_jdbc_to_gs.py'
    ,cluster_name='mydataproc2',region='us-central1',
    dataproc_pyspark_jars=['gs://mysqlnosql/spark-avro.jar'], dag=dag)    

t2 = DataProcHiveOperator(query=query_part,
cluster_name='mydataproc2',region='us-central1',task_id='create_table_in_hive_2_cols',dag=dag)

t3 = DataProcHiveOperator(query=query_all,
cluster_name='mydataproc2',region='us-central1',task_id='create_table_in_hive_all_cols',dag=dag)

t4 = DataFlowPythonOperator(py_file='gs://mysqlnosql/beam_gcs_bt.py',task_id='loadfrom-gcs-to-bt',
           dataflow_default_options={'project': '<yourprojectid>'},
           options={'avro_input':'gs://mysqldataflow/avro/customer/','json_input':'gs://mysqldataflow/json/customer/'},dag=dag

)

t5 = DataFlowPythonOperator(py_file='gs://mysqlnosql/beam_gcs_datastore.py',task_id='loadfrom-gcs-to-datastore',
           dataflow_default_options={'project': '<yourprojectid>'},
           options={'json_input':'gs://mysqldataflow/json/customer/'},dag=dag

)

t1 >> t2
t1 >> t3
t1 >> t4
t1 >> t5