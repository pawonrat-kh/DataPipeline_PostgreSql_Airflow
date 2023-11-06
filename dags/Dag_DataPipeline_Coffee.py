from airflow import DAG
from datetime import datetime,timedelta
import pendulum
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.xcom import XCom
from sqlalchemy import create_engine, Column, ForeignKey, DDL
from sqlalchemy.types import Integer, DateTime, VARCHAR, DECIMAL, CHAR, Time
from sqlalchemy.orm import relationship,declarative_base,Session

def connect_postgres(ti=None):
    #Connect to Postgres Database
    try:
        sqluser = 'postgres'
        sqlpass = 'docker'
        dbname = 'postgres'
        ti.xcom_push(key='sqluser', value=sqluser) #push variable in XCOM
        ti.xcom_push(key='sqlpass', value=sqlpass)
        ti.xcom_push(key='dbname', value=dbname)
        create_engine(f'postgresql://{sqluser}:{sqlpass}@postgres/{dbname}')
        print('Connected to Postgres.')
    except:
        raise ValueError('Cannot connect to Postgres.')

def check_table(ti=None):
    try:
        sqluser = ti.xcom_pull(task_ids='Connect_to_Postgres',key='sqluser') #pull variable in XCOM to use in another tasks
        sqlpass = ti.xcom_pull(task_ids='Connect_to_Postgres',key='sqlpass')
        dbname = ti.xcom_pull(task_ids='Connect_to_Postgres',key='dbname')
        engine = create_engine(f'postgresql://{sqluser}:{sqlpass}@postgres/{dbname}')
        session = Session(engine)
        table_name = ['staff','sales_outlet','customer','product_type','product','sales_transaction','sales_detail']
        def check_table(table_name,session):
            script = f'DROP TABLE IF EXISTS "{table_name}";'
            session.execute(DDL(script)) #execute sql script for dropping table if exits
            session.commit() 
            print(f'If {table_name} table exists then drop table.')
        #Check if table exists
        for i in range(len(table_name)):
            check_table(table_name[i],session)
    except:
        raise ValueError('Cannot check exit tables')

def create_table(ti=None):
    try:
        sqluser = ti.xcom_pull(task_ids='Connect_to_Postgres',key='sqluser') #pull variable in XCOM to use in another tasks
        sqlpass = ti.xcom_pull(task_ids='Connect_to_Postgres',key='sqlpass')
        dbname = ti.xcom_pull(task_ids='Connect_to_Postgres',key='dbname')
        engine = create_engine(f'postgresql://{sqluser}:{sqlpass}@postgres/{dbname}')
        Base = declarative_base()
        #Create tables, data type, and relationships
        class Staff(Base):
            __tablename__ = 'staff'
            staff_id = Column(Integer,primary_key=True,nullable=False) 
            first_name = Column(VARCHAR(50),nullable=False)
            last_name = Column(VARCHAR(50),nullable=False)
            position = Column(VARCHAR(50),nullable=False)
            start_date = Column(DateTime,nullable=False)
            location = Column(VARCHAR(50),nullable=False)

        class Sales_outlet(Base):
            __tablename__ = 'sales_outlet'
            sales_outlet_id = Column(Integer,primary_key=True,nullable=False) 
            sales_outlet_type = Column(VARCHAR(50),nullable=False)
            address = Column(VARCHAR(50),nullable=False)
            city = Column(VARCHAR(50),nullable=False)
            telephone = Column(VARCHAR(15),nullable=False)
            postal_code = Column(Integer,nullable=False)
            manager = Column(Integer)

        class Customer(Base):
            __tablename__ = 'customer'
            customer_id = Column(Integer,primary_key=True,nullable=False) 
            customer_name = Column(VARCHAR(50),nullable=False)
            email = Column(VARCHAR(50),nullable=False)
            reg_date = Column(DateTime,nullable=False)
            card_number = Column(VARCHAR(15),nullable=False)
            date_of_birth = Column(DateTime,nullable=False)
            gender = Column(CHAR(1),nullable=False)

        class Sales_detail(Base):
            __tablename__ = 'sales_detail'
            sales_detail_id = Column(Integer,primary_key=True,nullable=False) 
            transaction_id = Column(Integer,ForeignKey('sales_transaction.transaction_id'),nullable=False)
            product_id = Column(Integer,ForeignKey('product.product_id'),nullable=False)
            quantity = Column(Integer,nullable=False)
            price = Column(DECIMAL(2),nullable=False)

        class Product(Base):
            __tablename__ = 'product'
            product_id = Column(Integer,primary_key=True,nullable=False) 
            product_name = Column(VARCHAR(150),nullable=False)
            description = Column(VARCHAR(250))
            product_price = Column(DECIMAL(2),nullable=False)
            product_type_id = Column(Integer,ForeignKey('product_type.product_type_id'),nullable=False)

        class Product_type(Base):
            __tablename__ = 'product_type'
            product_type_id = Column(Integer,primary_key=True,nullable=False) 
            product_type = Column(VARCHAR(50),nullable=False)
            product_category = Column(VARCHAR(50),nullable=False)

        class Sales_transaction(Base):
            __tablename__ = 'sales_transaction'
            transaction_id = Column(Integer,primary_key=True,nullable=False) 
            transaction_date = Column(DateTime,nullable=False)
            transaction_time = Column(Time)
            sales_outlet_id = Column(Integer,ForeignKey('sales_outlet.sales_outlet_id'),nullable=False)
            staff_id = Column(Integer,ForeignKey('staff.staff_id'),nullable=False) 
            customer_id = Column(Integer,ForeignKey('customer.customer_id'),nullable=False)
        Base.metadata.create_all(engine)
        print('Tables are created completely.')
    except:
        raise ValueError('Cannot create tables on Database.')

def insert_data(ti=None):
    sqluser = ti.xcom_pull(task_ids='Connect_to_Postgres',key='sqluser') #pull variable in XCOM to use in another tasks
    sqlpass = ti.xcom_pull(task_ids='Connect_to_Postgres',key='sqlpass')
    dbname = ti.xcom_pull(task_ids='Connect_to_Postgres',key='dbname')
    engine = create_engine(f'postgresql://{sqluser}:{sqlpass}@postgres/{dbname}')
    session = Session(engine)
    try:
        with open('/opt/airflow/dags/CoffeeData.txt','r') as file:
            for item in file:
                script = f'{item}'
                session.execute(DDL(script)) #execute sql script for dropping table if exits
                session.commit()
            file.close()
        print('Insert all data to Database completely.')
    except:
        raise ValueError('Failed to insert data to Database.')

default_args = {
    'owner' : 'Pawonrat.kh',
    'email' : <your email>,
    'email_on_failure': True,
    'email_on_retry': True,
    'default_view' : 'graph',
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5)
}

dag = DAG(
    dag_id = 'Dag_DataPipeline_Coffee_transactions',
    default_args = default_args,
    description = 'Data pipeline for getting coffee sales transaction and creating tables in order to insert transaction data',
    start_date = pendulum.datetime(2023, 11, 2,tz='Asia/Bangkok'), #set time to Thailand time
    schedule_interval = '@daily'
) 

connect_postgres = PythonOperator(
    task_id = 'Connect_to_Postgres',
    python_callable = connect_postgres,
    dag = dag
)

check_table = PythonOperator(
    task_id = 'Checking_tables_if_exits_then_drop',
    python_callable = check_table,
    dag = dag
)

create_table = PythonOperator(
    task_id = 'Create_tables',
    python_callable = create_table,
    dag = dag
)

download_data = BashOperator(
    task_id = 'Download_data',
    bash_command='''pip install gdown 
                cd /opt/airflow/dags
                gdown "https://drive.google.com/uc?/export=download&id=1U7yBKQAyeqDNTvhJSEY-d348FXJRFMHB"''',
    dag=dag
)

insert_data = PythonOperator(
    task_id = 'Insert_data',
    python_callable = insert_data,
    dag = dag
)

delete_file = BashOperator(
    task_id='Delete_file_after_inserted',
    bash_command='rm /opt/airflow/dags/CoffeeData.txt',
    dag=dag
)

connect_postgres >> check_table >> create_table
create_table >> download_data >> insert_data >> delete_file 
