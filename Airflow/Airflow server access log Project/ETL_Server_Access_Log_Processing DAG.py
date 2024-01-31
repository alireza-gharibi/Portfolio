# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago   # This makes scheduling easy

default_args = {
    'owner': 'alireza gharibi',
    'start_date': days_ago(0),
    'email': ['alireza@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"

# define the DAG:
dag = DAG(
    dag_id='ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL Server Access Log Processing DAG using Bash',
    schedule_interval=timedelta(days=1),
)

# define the tasks
make_directory = BashOperator(
    task_id='make_directory',
    bash_command="mkdir -p /opt/airflow/my_project/ ",  # preparing the staging area: creating a directory to contain the whole project files.
    dag=dag,
) 
# define the task named download:
download = BashOperator(
    task_id='download',
    bash_command=f"curl {URL} > /opt/airflow/my_project/web-server-access-log.txt",  #downloading the file. i can also use 'wget'
    dag=dag,
)
# define the task named extract:
extract = BashOperator(
    task_id= 'extract',
    bash_command='cut -d "#" -f1,4 /opt/airflow/my_project/web-server-access-log.txt > /opt/airflow/my_project/extracted-log-data.txt ',  # extracting two columns of the table
    dag=dag,
)
# define the task named transform:
transform = BashOperator(
    task_id='transform',
    bash_command='tr "[A-Z]" "[a-z]"  < /opt/airflow/my_project/extracted-log-data.txt > /opt/airflow/my_project/transformed-log-data.txt ', # make 'visitorid' column lower-case
    dag=dag,
)
# define the task named load:
load = BashOperator(
    task_id='load',
    bash_command='tar -czvf /opt/airflow/my_project/log.tar.gz /opt/airflow/my_project/transformed-log-data.txt', #compressing and archiving the file
    dag=dag,
)


make_directory >> download >> extract >> transform >> load     # task pipeline


