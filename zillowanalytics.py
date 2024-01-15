# import libraries
from airflow import DAG
from datetime import timedelta, datetime
import json
import requests
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


# load json config file
with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)

# datetime now 
now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

# cleaned intermediate s3 bucket 
s3_bucket = 'cleaned-data-csv-bucket3'

# function to extract the api input values 
def extract_zillow_data(**kwargs):

    # extract the inputs entered in the python operator 
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']

    # return headers, get request to fetch the data
    response = requests.get(url, headers=headers, params=querystring)
    # obtain the data in json format
    response_data = response.json()
    
    # specify the output file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'

    # write the json response to a file
    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=4)  # indent for formatting
    output_list = [output_file_path, file_str]
    return output_list   

# defining the default arguments 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

# creating the DAG 
with DAG('zillow_analytics_dag',
        default_args = default_args,
        schedule_interval = '@daily',               # scheduled daily 
        catchup = False) as dag:

        # creating the python operator for retrieving the data from the provided link 
        extract_zillow_data_var = PythonOperator(
        task_id = 'tsk_extract_zillow_data_var',     # task name
        python_callable = extract_zillow_data,       # call extract zillow data function 
        op_kwargs = {'url': 'https://zillow56.p.rapidapi.com/search',   # api url
                   'querystring': {"location":"houston, tx"},           # api querystring
                   'headers': api_host_key,                             # headers
                   'date_string':dt_now_string}                         # date string 
        )

        # create a bash operator for moving the data file from the ec2 to s3
        load_to_s3 = BashOperator(
             task_id = 'tsk_load_to_s3',                # task name
            # xcom allows tasks to communicate: this command pulls the output of previous task for the file path
            # aws command to move the file from local system to the s3 bucket 
             bash_command = 'aws s3 mv {{ ti.xcom_pull("tsk_extract_zillow_data_var")[0]}} s3://endtoendyoutube-ym-bucket3/',
        )

        # create a key sensor to check the intermediate bucket has any file in it to transform it 
        is_file_in_s3_available = S3KeySensor(
             task_id = 'tsk_is_file_available_in_s3', 
             # xcom allows tasks to communicate: this command pulls the output of previous task for the file path
             bucket_key = '{{ti.xcom_pull("tsk_extract_zillow_data_var")[1]}}',     # csv file name 
             bucket_name = s3_bucket, 
             aws_conn_id = 'aws_s3_conn2',  # aws connection id 
             wildcard_match = False,        # true if we want to use wildcards in prefix
             timeout = 120,                 # timeout for the sensor in seconds 
             poke_interval = 5,             # time interval between s3 checks in seconds 
        )

        # create a task to transfer data from s3 to redshift once the sensor gets the data
        transfer_s3_to_redshift = S3ToRedshiftOperator(
             task_id = 'task_transfer_s3_to_redshift',              # task id 
             aws_conn_id = 'aws_s3_conn2',                          # aws connection id (need access to s3)
             redshift_conn_id = 'conn_id_redshift2',                 # redshift connection id 
             s3_bucket = s3_bucket,                                 # the s3 bucket to fetch data from 
             s3_key = '{{ti.xcom_pull("tsk_extract_zillow_data_var")[1]}}',     # csv file name 
             schema = "PUBLIC",                                     
             table = "zillowdata",                                  # table where data needs to be uploaded 
             copy_options = ["csv IGNOREHEADER 1"],                 # not to add the header of the csv file 

        )



        # creating the dag with directions 
        extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift