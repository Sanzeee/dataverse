import airflow
import csv
import datetime
import logging
import os
import pendulum
import sys

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models import DagBag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator , BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from csv import reader

from pl_mkite_refdata_refresh_subdag import my_sub_dag_def

default_args = {
    'owner': 'DynamicMapping',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'provide_context': True,
    'start_date': datetime.datetime(2021, 4, 13),
	'pool':'dm_refresh_ref_data',
    #'start_date': datetime.datetime(2019, 1, 1),
    #'start_date':pendulum.datetime.yesterday().astimezone('Europe/Berlin'),
    #'on_failure_callback': update_failed_task,
}


dm_config = Variable.get("dynamic_mapping_refdata_config", deserialize_json=True)
DMdb = dm_config ["db_connection_id"]
reference_table_schema = dm_config["ref_schema"]
DM_schema = dm_config["metadata_schema"]
reference_table = dm_config["reference_table"]
tablelist= []
taskname = 'pl_mkite_refdata_refresh'

with DAG(taskname,catchup=False,start_date = datetime.datetime(2021, 4, 13),schedule_interval = '*/15 * * * 1-5', default_args = default_args) as dag:     

    def get_list_of_referencetables(**kwargs):
        #task_instance = kwargs['ti']
        pgsql = PostgresHook(postgres_conn_id=DMdb)
        #Fetch the application maintained reference tables
        sqlText = " select ref_name FROM " + DM_schema + "." + reference_table + "  where maintained_by = 'Application' and (DATE_PART('day', CURRENT_TIMESTAMP  - lastupdated_dt) * 24 + DATE_PART('hour', CURRENT_TIMESTAMP  - lastupdated_dt) )* 60 + DATE_PART('minute', CURRENT_TIMESTAMP  - lastupdated_dt ) > refresh_frequency  ;"
        records = pgsql.get_records(sqlText)
        tables = [item for t in records for item in t]
        tablelist = ",".join(tables)
        #print('tablelist {}'.format(tablelist))
        #set variable
        if records is not None and len(records)>0 :
            Variable.set("dynamic_mapping_reference_table_list", tablelist)
            #task_instance.xcom_push(key="tablenamelist", value= tablelist) 
            return 'sub_dag'
        else :
            return 't_dummy_operator'
            
        
        
    
    t_dummy_operator = DummyOperator(task_id='t_dummy_operator', dag=dag,default_args = default_args)

    
    sub_dag = SubDagOperator(
        subdag=my_sub_dag_def(taskname, 'sub_dag', default_args),
        task_id='sub_dag',
        dag=dag,
    )

    t_template_read_params = BranchPythonOperator(
        task_id='t_template_read_params',
        provide_context=True,
        python_callable=get_list_of_referencetables,
        dag=dag,
        default_args = default_args)    

    t_template_read_params >> sub_dag
    t_template_read_params >> t_dummy_operator
