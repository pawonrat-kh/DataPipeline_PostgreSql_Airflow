# DataPipeline_PostgreSql_Airflow

Hello, My name is Pawonrat Khumngoen. In this repository, I will show and walk through my personal project, which is a data pipeline. In this project, I developed a data pipeline using a case from previous work about getting files from One Drive and transferring them to Google storage. 

After I researched and learned more Data engineering skills from Coursera, I am using this knowledge composed of Data modeling, Data pipeline, Databases, Shell scripting, and Apache Airflow to design and create data pipeline simulation. I used sale coffee transaction data that is stored on Google Drive so the others can access, download, or do step-by-step following my data pipeline easier.

I used a data pipeline tool, which is Apache Airflow to run the pipeline, set a daily schedule, and send email notifications of DAG status when tasks failed, retried, or success. In addition, I used the PostgreSQL database to store data and Docker for running Apache Airflow. 

To install Apache Airflow on Docker, you can follow by steps at https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html.

This is a data pipeline overview:

![Coffee DataPipeline drawio (1)](https://github.com/pawonrat-kh/DataPipeline_PostgreSql_Airflow/assets/90255313/9d951855-fc3b-463a-8fda-ed1ce8bd8f75)

In the data pipeline, I have 6 tasks.
* 1. Connect to PostgreSQL: For checking database connection status.
  2. Checking tables if exits then drop the table: In this pipeline, I will drop tables that exits. If you don't need this task, you can skip it.
  3. Create tables: I designed an ER diagram from transaction data and normalization tables. I used SQLalchemy library to set relationships between tables, data types, primary key, foreign key, etc.
  4. Download data: I used the Bash command to download sale coffee transaction data which is a txt format file composed of text insert data SQL command from URL.
  5. Insert data: I inserted data by reading line by line from txt file and this task will take time around 20 mins.
  6. Delete file after inserted: I deleted the file after the insert task finished.
  When all tasks are successful or some tasks fail it will send a notification to the email that you set.

This is the result of the DAG graph when tasks are completely successful:

![image](https://github.com/pawonrat-kh/DataPipeline_PostgreSql_Airflow/assets/90255313/760f676f-c05e-4a13-8763-1b43c69e91ff)

## The problem that I found and resolved it
* 1. The first time on the download data task, I was going to use the wget library in Python but I found a problem when I installed the wget PyPI version in requirements.txt also in Dockerfile and then ran the Docker. It did not recognize the wget library so I used the Bash command to download data instead.
  2. The first time on sent email notifications about task status, I used Airflow EmailOperator but it had limited about setting details of the task status, thus I used to set parameters in Environment Airflow or docker-compose.yaml file instead.

## Skills
* 1. Python
  2. SQL
  3. Apache Airflow
  4. Docker
  5. Data modeling
  6. Data pipeline
  7. Databases
  8. Shell scripting
     
