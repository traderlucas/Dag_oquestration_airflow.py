from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable 
import sqlite3
import pandas as pd


my_email = Variable.get("my_email").split(',')
 
def output_orders():
  con = sqlite3.connect("data/Northwhind_small.sqlite")

  cur = con.cursor()

  cur.execute('SELECT * from "Order"')
  df = cur.fetchall()
  df = pd.DataFrame(df)
  df.to_csv('/home/lucas/Documents/airflow_tooltorial/data/output_orders.csv')

  con.close()

def order_detail():
  con = sqlite3.connect("data/Northwhind_small.sqlite")

  cur = con.cursor()

  cur.execute('SELECT * From OrderDetail;')
  response = cur.fetchall()
  df = pd.DataFrame(response)
  df.to_csv('/home/lucas/Documents/airflow_tooltorial/data/output_orders_detail.csv')
 

  con.close()

def final_output():
  Orders = pd.read_csv ('/content/orders.csv',index_col=None)
  Order_details = pd.read_csv('/content/output_orders_details.csv',index_col=None)
  final = pd.merge(Orders, Order_details,left_on='0', right_on='1' )
  count = final.loc[final['10']=='Rio de Janeiro']['4_y'].sum()
  count = str(count)
  with open('count.txt', 'w') as f:
    f.write(count)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email':my_email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'NorthwindELT',
    default_args=default_args,
    description='A ELT dag for the Northwind ECommerceData',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 2, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        ELT Diária do banco de dados de ecommerce Northwind,
        começando em 2022-02-07. 
    """

    output_orders= PythonOperator(
        task_id='output_orders',
        python_callable = output_orders,
    )

    order_detail = PythonOperator(
        task_id='order_detail',
        python_callable = order_detail,
    )

    final_output = PythonOperator(
        task_id='final_output',
        python_callable = final_output,
    )

output_orders >> order_detail >>final_output

