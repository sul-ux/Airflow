import pendulum
import pandas as pd
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from airflow.sdk import DAG, task

with DAG(
    dag_id = "Customer_data_dag",
    start_date = pendulum.datetime(2026,10,11, tz= "Asia/Kolkata"),
    schedule = "0 0 * * *", # first 0 -> minute, second 0 -> hour
    catchup = False,
) as dag:
  @task
  
  #data base connection
  
  def run_sql():
    # to get schedule date for each day report with date in file name
    context = get_current_context()
     
    date = context['data_interval_start'].to_date_string()
      
    docker_db = PostgresHook(postgres_conn_id="docker_postgres")
    Customer_data = docker_db.get_pandas_df("SELECT * FROM customers;")
    print("Query to execute:", Customer_data)
    
    output_file_path = f"/opt/airflow/reports/customer_data_{date}.xlsx"
    Customer_data.to_excel(output_file_path, index=False)
    print("Excel file saved to:", output_file_path)
  run_sql()
  