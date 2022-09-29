# to start docker container run this on terminal
# once docker is up, ./stop.sh first, make sure the docker UI does not have any container,if not the containers would be unhealthy
# type this in terminal ./start.sh

# so whatever environment you are in, you use the airflow container, but the files in airflow will not correspond to the files from airflow-section-3 folder

# docker exec -it 1c2669e2f63c /bin/bash
# pip install yahoo_fin
#  pip install ta
# when you first get into the airflow container

# hue http://localhost:32762/
# airflow UI http://localhost:8080/


from airflow import DAG
# aws use a different provider
# list of all providers: https://airflow.apache.org/docs/apache-airflow-providers/packages-ref.html
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import csv
import requests
import json

from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta, date
import os

from yahoo_fin.stock_info import get_data
import pandas as pd
from ta import add_all_ta_features
from ta.utils import dropna


# some configuration
date_second = date.today()
date_first = date.today() + timedelta(days=-90)
stock_ = "AAPL"

directory_='/opt/airflow/dags/files'



# specify dictionary default_args corresponding to the default arguments that you want to apply to all your tasks
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 3,
    "retry_delay": timedelta(minutes = 5)
}


# remember that this is the actual directory "/opt/airflow/dags/files", its important
def remove_files(directory=directory_):
    
    for root, dirs, files in os.walk(directory):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

def download_csv(stock=stock_, date_1=date_first, date_2=date_second,directory=directory_,time_interval='1d'):
    df = get_data(stock, start_date=date_1, end_date=date_2, index_as_date = True, interval=time_interval)
    df = dropna(df)
    df = add_all_ta_features(
        df, open="open", high="high", low="low", close="adjclose", volume="volume", fillna=True)
    # df.to_csv(path_or_buf=f'{folder_to_save}/{stock}.csv',index=False)
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'date'})

    df.drop(df.columns.difference(['date','close','volume','momentum_rsi']), 1, inplace=True)

    folder_to_save = directory +'/' +'stock' +'.csv' 
    df.to_csv(folder_to_save,index=False)    
    print(df)




with DAG("stock_data_pipeline", start_date=datetime(2021,1,1), 
    schedule_interval="@daily", default_args = default_args, catchup=False) as dag:



    # 1. remove files in dag/files
    remove_original_files = PythonOperator(
        task_id="remove_original_files",
        python_callable=remove_files
    )

    # 2. get data yahoo finance api, transform the data using ta library and store it as csv
    store_stock_data = PythonOperator(
        task_id="store_stock_data",
        python_callable=download_csv
    )
    
    # 3. save the stock information into HDFS
    # && \ to execute anotehr command
    # on hue, it is under file/user
    saving_to_HDFS = BashOperator(
        task_id = "saving_to_HDFS",
        bash_command="""
            hdfs dfs -mkdir -p /user/portfolio && \
            hadoop fs -put -f /opt/airflow/dags/files/stock.csv /user/portfolio/stock.csv
            """

    )


# remember to establish hive conn
    # 4. save the hive table from local
    # this stores the data to the hive warehouse under files, not the database yet, you have to use spark for that
    creating_hive_table_from_csv_local = HiveOperator(
        task_id="creating_hive_table_from_csv_local",
        hive_cli_conn_id ="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS stocks_portfolio (
                `date` date,
                close float,
                volume float,
                momentum_rsi float 
                );

            LOAD DATA LOCAL INPATH '/opt/airflow/dags/files/stock.csv' INTO TABLE stocks_portfolio;
            SELECT * FROM stocks_portfolio;

        """
    )

    # 5. send email once the databases are updated
    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="dennisinvesting@gmail.com", #which email address you want to send to
        subject="stock database updated",
        html_content="<h3>the stock database has been updated</h3>"
    )

