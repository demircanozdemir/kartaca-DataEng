from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import json
import requests
import mysql.connector

dataNames = None # fonksiyonlar arası iletişim için değişken tanimlaması

default_args = { # varsayılan DAG argümanları
    'owner': 'kartaca',
    'retries':5,
    'retry_delay': timedelta(seconds=10)
}

# Veri tabanına veri girişini sağlamak için bir fonksiyon
#    user: kartaca
#    password: kartaca.123
def insertData(code,curr): 

    # conn = mysql.connector.connect(
    #      host = "localhost",
    #      user = "kartaca",
    #      password = "kartaca.123",
    #      database = "kartaca-db")
    conn = mysql.connector.connect(
         host = "localhost",
         user = "kartaca",
         password = "kartaca",
         database = "kartaca-db")
    
    mycursor = conn.cursor()
    sql = f"INSERT INTO country(code, name) VALUES(%s,%s)"
    values = (code, curr)
    mycursor.execute(sql,values)
    try:
        conn.commit()
    except mysql.connector.Error as err:
        print("error: ", err)
    finally:
        conn.close()


def start():  #task1 fonksiyonu
        print(' country_DAG calismaya basladi. ')

def read():   #task2 fonksiyonu
        global dataNames
        url = "http://country.io/names.json"
        dataNames = json.loads(requests.request("GET",url).text)

def insert(): #task3 fonksiyonu
    for code in dataNames:
        insertData(code, dataNames[code])

def end():    #task4 fonksiyonu
     print(' counrty_DAG sona erdi. ')


with DAG(
    default_args = default_args,
    dag_id = 'first',
    start_date = datetime(2023,4,2),  # 2 Nisandan itibaren
    schedule_interval  = '0 10 * * *' # Her gün 10:00'da
) as dag:
        task1 = PythonOperator(
            task_id = 'task_1',
            python_callable=start
        )

        task2 = PythonOperator(
            task_id = 'task_2',
            python_callable=read
        )

        task3 = PythonOperator(
            task_id = 'task_3',
            python_callable=insert
        )

        task4 = PythonOperator(
            task_id = 'task_4',
            python_callable=read
        )


        task1>>task2>>task3>>task4

