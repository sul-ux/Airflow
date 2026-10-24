import pendulum
from airflow.sdk import DAG, task

with DAG(
    dag_id = "daily_sql_dag",
    start_date = pendulum.datetime(2026,10,11, tz= "Asia/Kolkata"),
    schedule = "0 0 * * *", # first 0 -> minute, second 0 -> hour
    catchup = False,
) as dag:
  @task
  def run_sql():
    result = "SELECT CURRENT_TIMESTAMP;"
    print("Query to execute:", result)
    
  run_sql()
  