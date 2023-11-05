# DataPipeline_PostgreSql_Airflow

Hello, My name is Pawonrat Khumngoen. In this repository, I will show and walk through my personal project, which is a data pipeline. In this project, I developed a data pipeline using a case from previous work about getting files from One Drive and transferring them to Google storage. 

After I researched and learned more data engineer skills from Coursera, I am using this knowledge composed of Data modeling, Data pipeline, Databases, Shell scripting, and Apache Airflow to design and create data pipeline simulation. I used sale coffee transaction data that is stored on Google Drive so the others can access, download, or do step-by-step following my data pipeline easier.

I used a data pipeline tool, which is Apache Airflow to run the pipeline, set a schedule, and send notifications of DAG status when tasks failed, retried, or success. In addition, I used the PostgreSQL database to store data and Docker for running Apache Airflow.

![Coffee DataPipeline drawio (1)](https://github.com/pawonrat-kh/DataPipeline_PostgreSql_Airflow/assets/90255313/9d951855-fc3b-463a-8fda-ed1ce8bd8f75)

In the data pipeline, I have 6 tasks.
* 1. Connect to PostgreSQL: For checking database connection status.
  2. Checking tables if exits then drop the table: In this pipeline, I will drop tables that exits. If you don't need this task, you can skip it.
  3. Create tables: I designed an ER diagram from transaction data and normalization tables. I used SQLalchemy library to set relationships between tables, data types, primary key, foreign key, etc.
  4. Download data: I used the bash command to download sale coffee transaction data which is a txt format file composed of text insert data SQL command from URL.
  5. Insert data: I inserted data by reading line by line from txt file and this task will take time around 20 mins.
  6. Delete file after inserted: I deleted the file after the insert task finished.

