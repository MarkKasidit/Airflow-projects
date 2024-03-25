from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from operators.custom_operators import CustomSensor
from utils.logger import Logger
from utils.db_connection import PostgresConnector

logger = Logger("airflow-project").get()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_query():
    try:
        script = """
            SELECT count(*) from products;
        """
        db = PostgresConnector()
        logger.info(f"Executing... {script}")
        result = db.runquery(script)
        logger.info({result[0][0]})
        db.close()
        logger.info(f"Execute query successfully.")
        return result
    except:
        logger.exception('Error')   
        raise Exception  

with DAG (
    dag_id="custom_sensor",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    start = DummyOperator(task_id = "start")
    
    my_sensor = CustomSensor(
        task_id = "my_sensor",
        poke_interval=60,  # Specify how often to check the condition (in seconds)
        timeout=120,  # Specify the timeout for the sensor (in seconds)
        python_callable=check_query

    )

    end = DummyOperator(task_id = "end")

    start >> my_sensor >> end
    