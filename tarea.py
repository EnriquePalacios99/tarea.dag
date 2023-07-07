from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'datapath',
}

with DAG(
    dag_id='pipeline_dag',
    default_args=default_args,
    start_date=datetime(2023, 6, 28),
    schedule_interval=None,
    catchup=False,
    template_searchpath=['/path/to/sql/files']
) as dag:

    create_table_a = PostgresOperator(
        task_id='create_table_a',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS table_a (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
        """
    )

    create_table_b = PostgresOperator(
        task_id='create_table_b',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS table_b (
            id SERIAL PRIMARY KEY,
            key_id INT NOT NULL);
        """
    )

    insert_data_table_a = PostgresOperator(
        task_id='insert_data_table_a',
        postgres_conn_id='postgres_localhost',
        sql="""
            INSERT INTO table_a (name, pet_type, birth_date, OWNER)
            VALUES ('Max', 'Dog', '2018-07-05', 'Jane'),
                   ('Susie', 'Cat', '2019-05-01', 'Phil');
        """
    )

    insert_data_table_b = PostgresOperator(
        task_id='insert_data_table_b',
        postgres_conn_id='postgres_localhost',
        sql="""
            INSERT INTO table_b (key_id)
            VALUES (1), (2);
        """
    )

    join_tables = PostgresOperator(
        task_id='join_tables',
        postgres_conn_id='postgres_localhost',
        sql="""
            SELECT * FROM table_a
            INNER JOIN table_b ON table_a.id = table_b.key_id;
        """
    )

    select_with_condition = PostgresOperator(
        task_id='select_with_condition',
        postgres_conn_id='postgres_localhost',
        sql="""
            SELECT * FROM table_a
            INNER JOIN table_b ON table_a.id = table_b.key_id
            WHERE table_a.name = 'Max';
        """
    )

    create_table_a >> create_table_b
    create_table_a >> insert_data_table_a
    create_table_b >> insert_data_table_b
    insert_data_table_a >> join_tables
    insert_data_table_b >> join_tables
    join_tables >> select_with_condition