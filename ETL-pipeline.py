# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
from urllib.parse import urlencode

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(query='Select 1'):
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230520',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
    df = ph.read_clickhouse(query, connection=connection)
    return df

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'k.tipalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 13),
}

# Интервал запуска DAG
schedule_interval = '0 8 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def k_tipalov_load():

    @task()
    def extract():
        query = """WITH tab1 as (SELECT
                                    a.event_date AS event_date,
                                    a.user_id AS user_id,
                                    a.quant_view AS views,
                                    a.quant_like AS likes,
                                    b.quant_message_sent AS messages_sent,
                                    b.quant_human_sent AS users_sent,
                                    c.quant_message_received AS messages_received,
                                    c.quant_human_received AS users_received
                                FROM
                                    (SELECT
                                        user_id,
                                        toDate(time) AS event_date,
                                        countIf(action='view') AS quant_view,
                                        countIf(action='like') AS quant_like
                                    FROM simulator_20230520.feed_actions
                                    WHERE toDate(time) = today() - 1
                                    GROUP BY user_id,event_date) a
                                    LEFT JOIN
                                    (SELECT
                                        user_id,
                                        toDate(time) AS event_date,
                                        count(reciever_id) AS quant_message_sent,
                                        uniqExact(reciever_id) AS quant_human_sent
                                    FROM simulator_20230520.message_actions
                                    WHERE toDate(time) = today() - 1
                                    GROUP BY user_id,,event_date) b
                                    USING user_id
                                    LEFT JOIN
                                    (SELECT
                                        reciever_id,
                                        toDate(time) AS event_date,
                                        count(user_id) AS quant_message_received,
                                        uniqExact(user_id) AS quant_human_received
                                    FROM simulator_20230520.message_actions
                                    WHERE toDate(time) = today() - 1
                                    GROUP BY reciever_id, event_date) c
                                    ON a.user_id = c.reciever_id)

                    SELECT
                        *
                    FROM tab1
                        left join
                        (SELECT
                            user_id,
                            gender,
                            age,
                            os
                        FROM simulator_20230520.message_actions) a
                        using user_id
                                """
        df = ch_get_df(query=query)
        return df

    @task
    def dimension_os(df):
        df_os = df.groupby(['event_date', 'os'])\
            .agg({'views': 'sum', 'likes': 'sum', 'messages_sent': 'sum','messages_received': 'sum', 'users_sent': 'sum', 'users_received': 'sum'})\
            .reset_index().rename(columns = {'os': 'dimension_value'})
        df_os.insert(1, 'dimension', 'os')
        return df_os
    

    @task
    def dimension_gender(df):
        df_gender = df.groupby(['event_date', 'gender'])\
            .agg({'views': 'sum', 'likes': 'sum', 'messages_sent': 'sum','messages_received': 'sum', 'users_sent': 'sum', 'users_received': 'sum'})\
            .reset_index().rename(columns = {'gender': 'dimension_value'})
        df_gender.insert(1, 'dimension', 'gender')
        return df_gender
    
    @task
    def dimension_age(df):
        df_age = df.groupby(['event_date', 'age'])\
            .agg({'views': 'sum', 'likes': 'sum', 'messages_sent': 'sum','messages_received': 'sum', 'users_sent': 'sum', 'users_received': 'sum'})\
            .reset_index().rename(columns = {'age': 'dimension_value'})
        df_age.insert(1, 'dimension', 'age')
        return df_age
    
    @task
    def load_to_clickhouse(df_os, df_gender, df_age):
        result = pd.concat([df_os, df_gender, df_age], axis=0).astype({'dimension': str,
                                                                        'dimension_value': str,
                                                                        'views': int,
                                                                        'likes': int,
                                                                        'messages_sent' : int,
                                                                        'messages_received' : int,
                                                                        'users_sent': int,
                                                                        'users_received': int})
        connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'}
        
        query_created = '''
                CREATE TABLE if not exists test.k_tipalov (
                event_date DATE,
                dimension TEXT,
                dimension_value TEXT,
                views int,
                likes int,
                messages_sent int,
                messages_received int,
                users_sent int,
                users_received int)
                ENGINE = MergeTree()
                order by event_date

                '''
        ph.execute(query_created, connection=connection_test)
        ph.to_clickhouse(result, 'k_tipalov', connection=connection_test, index=False)
   
    

    df = extract()
    dimension_os = dimension_os(df)
    dimension_gender = dimension_gender(df)
    dimension_age = dimension_age(df)
    
    load_to_clickhouse(df_os, df_gender, df_age)

k_tipalov_load = k_tipalov_load()
