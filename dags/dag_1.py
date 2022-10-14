import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Daniel_Trezena",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 10)
}


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['Titanic'])
def dag_1():

    @task
    def ingestao():
        NOME_TABELA = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_passag_1(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {"PassengerId": "count"}).reset_index().rename(columns={'PassengerId': 'Passengers'})
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_passag_2(nome_do_arquivo):
        NOME_TABELA = "/tmp/preco_medio_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {'Fare': 'mean'}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_passag_3(nome_do_arquivo):
        NOME_TABELA = "/tmp/familiares_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df['SibSp_Parch'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg(
            {'SibSp_Parch': 'sum'}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_total(nome_do_arquivo_1, nome_arquivo_2, nome_do_arquivo_3):
        NOME_TABELA = "/tmp/tabela_unica.csv"
        df_ind_1 = pd.read_csv(nome_do_arquivo_1, sep=';')
        df_ind_2 = pd.read_csv(nome_arquivo_2, sep=';')
        df_ind_3 = pd.read_csv(nome_do_arquivo_3, sep=';')
        res = df_ind_1.merge(df_ind_2, how='inner', on=['Sex', 'Pclass'])
        res = res.merge(df_ind_3, how='inner', on=['Sex', 'Pclass'])
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=';')
        return NOME_TABELA

    ing = ingestao()
    ind_1 = ind_passag_1(ing)

    ind_2 = ind_passag_2(ing)

    ind_3 = ind_passag_3(ing)

    ind_total = ind_total(ind_1, ind_2, ind_3)

    triggerdag = TriggerDagRunOperator(
        task_id="trabalho_2_dag_2",
        trigger_dag_id="trabalho_2_dag_2"
    )

    fim = DummyOperator(task_id="fim")
    [ind_1, ind_2, ind_3] >> ind_total >> triggerdag >> fim


execucao = dag_1()
