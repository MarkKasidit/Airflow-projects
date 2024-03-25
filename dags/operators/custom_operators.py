from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import logging as logger
from utils.db_connection import PostgresConnector

class CustomSensor(BaseSensorOperator, PythonOperator):
    def __init__(self,python_callable, *args, **kwargs):
        super(CustomSensor, self).__init__(python_callable=python_callable, *args, **kwargs)
        # self.python_callable = python_callable
        
    def poke(self, context):
        try:
            self.result = self.python_callable()
            if not self.result:
                logger.info("Something wrong...")
                return False
            else:
                if str(self.result[0][0]) in ('0', ''):
                    logger.info("Condition not met")
                    return False
                else:
                    logger.info("Condition met")
                    return True
        except AirflowException:
            logger.error("Error occur...")
            return False
        