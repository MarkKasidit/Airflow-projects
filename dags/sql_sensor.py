from airflow import DAG
from airflow.sensors.sql_sensor import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG("sql_sensor",
         start_date = datetime(2023,1,1),
         schedule_interval = '@daily',
         catchup = False) as dag:
    
    retrieve_data = PostgresOperator(
        task_id = "retrieve_data",
        postgres_conn_id = "postgres",
        sql = '''
            select * from users;
        '''
    )

    test_sensor = SqlSensor(
        task_id = "task_sensor",
        conn_id = "postgres",
        poke_interval=60,
        timeout=120,
        sql = '''
            select count(*) from users
            where country = 'Norway';
        '''
    )

    test_sensor_2 = SqlSensor(
        task_id = "task_sensor_2",
        conn_id = "postgres",
        poke_interval=60,
        timeout=120,
        sql = '''
            select count(*) from users
            where country = 'Thailand';
        '''
    )

    end1 = DummyOperator(task_id = "end1")
    end2 = DummyOperator(task_id = "end2")

    retrieve_data >> test_sensor >> end1
    retrieve_data >> test_sensor_2 >> end2

    # extract_user = PostgresOperator(
    #     task_id = "extract_user",
    #     postgres_conn_id = "postgres",
    #     sql = '''
    #         drop table if exists users_norway;

    #         create table if not exists users_norway (
    #             firstname TEXT NOT NULL,
    #             lastname TEXT NOT NULL,
    #             country TEXT NOT NULL
    #         );

    #         insert into users_norway
    #         select firstname, lastname, country 
    #         from users
    #         where country = 'Norway'; 
    #     '''
    # )

    # extract_user_2 = PostgresOperator(
    #     task_id = "extract_user_2",
    #     postgres_conn_id = "postgres",
    #     sql = '''
    #         drop table if exists users_thai;

    #         create table if not exists users_thai (
    #             firstname TEXT NOT NULL,
    #             lastname TEXT NOT NULL,
    #             country TEXT NOT NULL
    #         );

    #         insert into users_thai
    #         select firstname, lastname, country 
    #         from users
    #         where country = 'Thailand'; 
    #     '''
    # )

    # retrieve_data >> test_sensor >> extract_user
    # retrieve_data >> test_sensor_2 >> extract_user_2
