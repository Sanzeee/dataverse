import airflow
import csv
import datetime
import logging
import os
import pendulum,json
import shutil

from airflow.models import Variable
from airflow.contrib.operators import bigquery_to_gcs
from airflow.hooks import postgres_hook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from csv import reader
from google.cloud import bigquery
from google.cloud import storage
from tempfile import NamedTemporaryFile

dynamictasks = []
dm_config = Variable.get("dynamic_mapping_refdata_config", deserialize_json=True)
DM_Project_name = dm_config["DM_Project_name"]
DMdb = dm_config ["db_connection_id"]
DM_schema = dm_config["metadata_schema"]
reference_table_schema = dm_config["ref_schema"]
reference_table = dm_config["reference_table"]
TargetDag = dm_config["TargetDag"]
GcpConnection = dm_config["GcpConnection"]
GCSBucket = dm_config["GCSBucket"]
subfolder = dm_config["subfolder"]
cloudstoragepath =  dm_config["cloudstoragepath"]
type_mysql = dm_config["type_mysql"]
type_postgres = dm_config["type_postgres"]
extn = dm_config["extn"]
sourcedetailstbl = dm_config["sourcedetailstbl"]
tablenamestr = Variable.get("dynamic_mapping_reference_table_list")
tasklist = list(tablenamestr.split(","))

default_args = {
        'owner': 'DynamicMapping',
        'depends_on_past': False,
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        #'provide_context': True,
        #'start_date': datetime.datetime(2021, 4, 8),
        'start_date' : datetime.datetime(2021, 4, 13),
        #'start_date':pendulum.datetime.yesterday().astimezone('Europe/Berlin'),
        #'on_failure_callback': update_failed_task,

    }
    
def retrieve_parent_value(retrieve_value,**kwargs):
        #retrieve_value = kwargs['dag_run'].conf['arguments']
        #print('retrieve value {}'.format(retrieve_value))
        #task_instance = kwargs['ti']
        #task_instance.xcom_push(key="tablename", value=retrieve_value)
        pgsql = PostgresHook(postgres_conn_id=DMdb)
        #sqlText = "select ref_name,source_type,project,source_database_dataset,connectionid from " + DM_schema + "." + reference_table + " a join " + DM_schema + "." + sourcedetailstbl +" b on a.msd_id = b.id where ref_name = '"+ retrieve_value + "'"
        sqlText = "select ref_name,source_type,project,source_database_dataset,source_table_name,connectionid, string_agg(arf_name,',' order by arf_seq) columnname from " + DM_schema + "." + "m_reference_tbl mrt join  " + DM_schema + "." + "m_source_details msd on msd.id = mrt.msd_id left join " + DM_schema + "." +"a_ref_fld_def arfd on mrt.id= arf_mrt_id where ref_name = '"+ retrieve_value + "' group by mrt.id,ref_name,source_type,project,source_database_dataset,connectionid limit 1"
        records = pgsql.get_records(sqlText)
        source_type = records[0][1]
        project = records[0][2]
        source_db = records[0][3]
        source_table = records[0][4]
        connectionid = records[0][5]
        columns = records[0][6]
        dest_table = records[0][0]
        if source_type.lower() == 'Bigquery'.lower():
            func =  RetrieveData(source_table,dest_table,project,source_db,connectionid,columns)
        elif source_type.lower() == 'cloudsql'.lower():
            conn = BaseHook.get_connection(connectionid)
            #print(conn.get_extra())
            connectiontype = json.loads(conn.get_extra())
            connection_type = connectiontype["database_type"]
            #print('conntype : {}'.format(connection_type))
            if connection_type == type_mysql:
                func =  generatecsvfile_from_mysql(source_table,dest_table,project,source_db,connectionid,columns)
            elif connection_type == type_postgres: 
                func =  generatecsvfile_from_postgres(source_table,dest_table,project,source_db,connectionid,columns)
        
    
def RetrieveData(table,dest_table,project,dataset,connectionid,columns,**kwargs):
    filename = table + extn
    try:
        client = bigquery.Client()
        #print(1)
        query = """select {columnlist} from `{pro}.{ds}.{tbl}` """.format(columnlist = columns ,pro=project,ds=dataset , tbl = table)
        #print(query)
        query_job = client.query(query)  # Make an API request.
        bqlist=[]
        #print("The query data:")

        for row in query_job:
         #Row values can be accessed by field name or index.
            bqlist.append(row[0])
        #print("List format for big query {}".format(bqlist))
        
        with open(filename, 'w') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',')
            #filewriter.writerow(headercolumns)
            for row in query_job:
                filewriter.writerow(row)
        client = storage.Client(project=DM_Project_name)
        bucket = client.get_bucket(GCSBucket)
        blob = bucket.blob(subfolder+filename)
        with open(filename, "rb") as my_file:
            blob.upload_from_file(my_file)
            
        func = copy_file_to_stg(dest_table)
        
    except Exception as e:
        exception_msg=str(e).replace("'","")
        #kwargs['ti'].xcom_push(key='exception_msg', value= exception_msg)
        raise RuntimeError(str(e))

def generatecsvfile_from_postgres(table,dest_table,project,dataset,connectionid,columns,**kwargs):
    filename = table + extn
    query='select ' + columns + ' from {schema}.{table}'.format(schema=dataset,table= table)
    #print('Get count from table :{}'.format(query))
    pg_hook = PostgresHook(postgres_conn_id= connectionid)
    results=pg_hook.get_records(query)
    cursor =  pg_hook.get_conn().cursor()
    copyquery= "COPY ({0}) TO STDOUT WITH (FORMAT csv, DELIMITER ',')".format(query)
    with open(filename, 'w') as f_output:
        cursor.copy_expert(copyquery, f_output)
    client = storage.Client(project=DM_Project_name)
    #bucket = client.get_bucket(bucketname)
    bucket = client.get_bucket(GCSBucket)
    blob = bucket.blob(subfolder+filename)
    with open(filename, "rb") as my_file:
        blob.upload_from_file(my_file)
     
    func = copy_file_to_stg(dest_table)
def generatecsvfile_from_mysql(table,dest_table,project,dataset,connectionid,columns,**kwargs):
    filename = table + extn
    #data = kwargs['ti']
    #table = task_instance.xcom_pull(task_ids='t_retrieve_parent_value', key = 'tablename')
    #project = task_instance.xcom_pull(task_ids='t_retrieve_parent_value', key = 'project')
    #dataset = task_instance.xcom_pull(task_ids='t_retrieve_parent_value', key = 'source_db')
    #connectionid = task_instance.xcom_pull(task_ids='t_retrieve_parent_value', key = 'connectionid')
    query='select ' + columns + ' from {schema}.{table}'.format(schema=dataset,table= table)
    #print('Get count from table :{}'.format(query))
    mysql_hook = MySqlHook(mysql_conn_id= connectionid)
    results=mysql_hook.get_records(query)
    cursor =  mysql_hook.get_conn().cursor()
    cursor.execute(query)
    with open(filename,"w") as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC ,delimiter=',')
        #writer.writerow(col[0] for col in cursor.description)
        for row in cursor:
            writer.writerow(row)
    client = storage.Client(project=DM_Project_name)
    #bucket = client.get_bucket(bucketname)
    bucket = client.get_bucket(GCSBucket)
    blob = bucket.blob(subfolder+filename)
    with open(filename, "rb") as my_file:
        blob.upload_from_file(my_file)
    func = copy_file_to_stg(dest_table)
        
def copy_file_to_stg(table_name,**kwargs):

    #data = kwargs['ti']
    #table_name = data.xcom_pull(task_ids='t_retrieve_parent_value', key = 'tablename')
    utempfile = None
    downloadfile = GoogleCloudStorageHook(google_cloud_storage_conn_id=GcpConnection,)
    with NamedTemporaryFile(mode = 'wb', delete=False) as dtmp:
        tempfile = None
        with NamedTemporaryFile(mode = 'w', suffix = 'csv', delete=False) as tmp:  
            try:
                downloadfile.download(bucket=GCSBucket,
                object=subfolder + table_name+ extn,
                filename = tmp.name)
                tempfile = tmp.name
                tmp.flush()
                tmp.seek(0)
                #print('tempfilename : {} '.format(tmp.name))

            except Exception as e:
                exception_msg=str(e)  
                #data.xcom_push(key='exception_msg', value= exception_msg) 
                raise RuntimeError(str(e))
                   
                
            
        try:
            
            with open(tempfile, mode='rb') as zfile:
                
                shutil.copyfileobj(zfile, dtmp)
        except Exception as e:
            exception_msg=str(e)  
            #data.xcom_push(key='exception_msg', value= exception_msg)

            
            
            raise RuntimeError(
                str(e)
            )
        finally:     
            os.remove(tempfile)
        utempfile = dtmp.name
        """with open(utempfile,'r') as read_obj:
            csv_reader = reader(read_obj)
            #for row in csv_reader:
                #print('read data :{}'.format(row))"""
    
    try:
        load_to_stage = PostgresHook(postgres_conn_id=DMdb)
        landingtable = table_name+'_landing'
        sqlTexttruncate = 'Truncate table {schema}.{table}'.format(schema=reference_table_schema,table=landingtable)
        Truncatelanding = CloudSqlQueryOperator(task_id = 't_copy_file_to_stage',
        sql = sqlTexttruncate ,
        gcp_cloudsql_conn_id = DMdb,
        ) 
        #print("testing error")
        load_to_stage.copy_expert("COPY {schema}.{table} FROM STDIN (FORMAT csv, DELIMITER ',')" .format(schema=reference_table_schema,table=table_name+'_landing'), filename = utempfile)
        sqlText = "call {schema}.refresh_tables( '{table}' );".format(schema=reference_table_schema,table = table_name)
        #print('sql: {}'.format(sqlText))
        processMeta = CloudSqlQueryOperator(task_id = 't_copy_file_to_stage',
        sql = sqlText ,
        gcp_cloudsql_conn_id = DMdb,
        )
        processMeta.execute(context= kwargs)



    except Exception as e:

        exception_msg=str(e)  
        #data.xcom_push(key='exception_msg', value= exception_msg)     
        raise RuntimeError(
            str(e)
        )
    finally:
        os.remove(utempfile)

def my_sub_dag_def(parent_dag_name, child_dag_name, args):
    # Step 1 - define the default parameters for the DAG
    


    my_sub_dag = DAG(dag_id = f'{parent_dag_name}.{child_dag_name}',
        catchup=False,
        start_date = datetime.datetime(2021, 4, 13),
        schedule_interval = '*/15 * * * *',
        default_args=default_args
    )


    if tasklist is not None and len(tasklist) > 0:
        for i in tasklist:   
            taskid = i
            tasks = PythonOperator(
                task_id='t_' + str(taskid),
                dag=my_sub_dag,
                provide_context=True,
                python_callable=retrieve_parent_value,
                op_args=[i])
            dynamictasks.append(tasks)

    return my_sub_dag
