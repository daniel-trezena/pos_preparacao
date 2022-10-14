import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


NOME_ARQUIVO = '/tmp/tabela_unica.csv'
default_args = {
    'owner': "Daniel_Trezena",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 10)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_2():

    @task
    def resultado():
        NOME_TABELA = '/tmp/resultados.csv'
        df = pd.read_csv(NOME_ARQUIVO, sep=';')
        res = df.agg({'Passengers': 'mean', 'Fare': 'mean', 'SibSp_Parch': 'mean'}).reset_index(
        ).rename(columns={'index': 'Indicator', 0: 'Mean_Value'})
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=';')
        return NOME_TABELA

    fim = DummyOperator(task_id="fim")
    resultado() >> fim


execucao = dag_2()
