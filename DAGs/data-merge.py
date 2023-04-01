from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import mysql.connector

default_args = {
    'owner': 'kartaca',
    'retries':5,
    'retry_delay': timedelta(seconds=10)
}


def start():  #task1 fonksiyonu
        print(' data-merge_DAG calismaya basladi. ')

def merge():  #task2 fonksiyonu
        mydb = mysql.connector.connect(
        host = "localhost", 
        user = "kartaca",
        password = "kartaca",
        database = "kartaca-db"
        )

        mycursor = mydb.cursor()
    #Tabloyu temizleme;
        mycursor.execute("Truncate table datamerge") 
        
    #Verileri tabloya yerleştirme;
        mycursor.execute("INSERT INTO datamerge (SELECT name, curr FROM country,currency WHERE country.code = currency.code)")
        mydb.commit()
        mydb.close()

def end():    #task3 fonksiyonu
     print(' data-merge_DAG sona erdi. ')


with DAG(
    default_args = default_args,
    dag_id = 'first',
    start_date = datetime(2023,4,2),   # 2 Nisandan itibaren
    schedule_interval  = '12 10 * * *' # Her gün 10:12'de
) as dag:
        task1 = PythonOperator(
            task_id = 'task_1',
            python_callable=start
        )
        task2 = PythonOperator(
            task_id = 'task_2',
            python_callable=merge
        )
        task3 = PythonOperator(
            task_id = 'task_3',
            python_callable=end
        )

        task1>>task2>>task3
