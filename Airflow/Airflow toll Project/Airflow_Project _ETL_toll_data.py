# import the libraries
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator 

# defining arguments
default_args = {
    'owner': 'alireza gharibi',
    'start_date': datetime.now(),
    'email': ['alireza@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"

parent_folder="/opt/airflow/dags/staging" # to make bash command shorter i use this as part of absolute paths


# define the DAG:
dag = DAG(
    dag_id = 'ETL_toll_data',
    default_args = default_args,
    description = ' Toll-data ETL ',
    schedule_interval = '@daily'
)

# define the tasks
make_directory = BashOperator(
    task_id='make_directory',
    bash_command="mkdir -p /opt/airflow/dags/staging",  # creating the staging area: creating a directory to contain the whole project files.
    dag=dag
) 
# define the task:
download = BashOperator(
    task_id ='download',
    bash_command = f"curl {URL} > {parent_folder}/toll_data.tgz",  #downloading the file. i can also use 'wget'
    dag = dag
)

# define the task:
unzip_data = BashOperator(
    task_id = 'unzip_data' ,
    bash_command = f"tar -xzf {parent_folder}/toll_data.tgz -C {parent_folder}" ,  #unzipping the file
    dag = dag
)

# define the task:
extract_data_from_csv = BashOperator(
    task_id= 'extract_data_from_csv',
    bash_command=f"cut -d ',' -f 1-4 {parent_folder}/vehicle-data.csv > {parent_folder}/csv_data.csv",  # extracting  columns of the csv file
    dag=dag
)
# define the task :
extract_data_from_tsv = BashOperator(
    task_id ='extract_data_from_tsv',
    bash_command = f"cut -f 5-7 --output-delimiter=',' {parent_folder}/tollplaza-data.tsv | dos2unix > {parent_folder}/tsv_data.csv", # extracting  columns of the tsv file, default delimiter for cut is 'tab'
    dag = dag                                      #(continued) this tsv file is not unix type it is widndows(dos) type. i convert it to unix then write it to file
)

# define the task :
extract_data_from_fixed_width = BashOperator(
    task_id ='extract_data_from_fixed_width',
    bash_command = f"cut -c 59-61,63-67 --output-delimiter=',' {parent_folder}/payment-data.txt > {parent_folder}/fixed_width_data.csv", # extracting  columns of the txt file
    dag = dag
)

# define the task :
consolidate_data = BashOperator(
    task_id ='consolidate_data',
    bash_command = f"paste -d ',' {parent_folder}/csv_data.csv {parent_folder}/tsv_data.csv {parent_folder}/fixed_width_data.csv > {parent_folder}/extracted_data.csv", # consolidating data from all the CSV files 
    dag = dag
)

transforming_column = BashOperator(
    task_id ='transforming_column',
    bash_command = f"cut -d ',' -f 4 {parent_folder}/extracted_data.csv | tr [a-z] [A-Z]  > {parent_folder}/transformed_column.csv", #  extracting field 4 and capitalizing it
    dag = dag                                                       
    )


load = BashOperator(
    task_id ='load',
    bash_command = f"cut -d ',' -f 5-9 {parent_folder}/extracted_data.csv > {parent_folder}/column_5_9_tmp.csv && cut -d ',' -f 1-3 {parent_folder}/extracted_data.csv | paste -d ',' - {parent_folder}/transformed_column.csv {parent_folder}/column_5_9_tmp.csv > {parent_folder}/final_data.csv", 
    dag = dag  # i use a temp file to store column 5 to 9. then using 'and' operator(&&) i continue. pipe the output of cut command to paste. '-' represents the pipe output in paste command file ordering.                                                                          
)


make_directory >> download >> unzip_data >> extract_data_from_csv  >> extract_data_from_tsv >> extract_data_from_fixed_width    # task pipeline
extract_data_from_fixed_width >> consolidate_data >> transforming_column >> load              # task pipeline