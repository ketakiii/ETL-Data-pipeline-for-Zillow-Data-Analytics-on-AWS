# ETL Data pipeline for Zillow Data Analytics

### Basic Pipeline 
Data pipeline from data extraction to building analytics dashboards to find insights from the extracted data. Here data is extracted, stored in S3, transformed, loaded to RedShift and finally to QuickSight for deriving insights through graphs and plots. Entire process done using the AWS services and Apache Airflow for orchestration of tasks. 
![alt text](images/data_pipeline.png)



## Extract
1. Used Rapid API (internally REST) to retrieve data in json format using the Python operator. 
2. Bash operator to move data from EC2 to S3 bucket. 
3. Created IAM Roles to give permission of one service to access another: 
![alt text](images/iam.png)

## Transform
1. Lambda function to copy the data to another S3 bucket. Triggered when file created in landing zone S3. 
![alt text](images/copy_lambda.png)

2. Lambda function to transform data, select necessary columns, row and convert to csv. 
![alt text](images/tranform_lambda.png)

## Load
1. Key Sensor checks for file in the cleaned S3 bucket. If found, moved to RedShift. 
![alt text](images/airflow.png)

2. Create a RedShift cluster and a table to store data in tabular format. 
![alt text](images/redshift.png)

3. Finally, connect QuickSight to RedShift to derive meaningful insights from the data. 
![alt text](images/analytics.png)

