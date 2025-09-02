import json
import boto3
import os
import pandas as pd
from sqlalchemy import create_engine

# def setup_db_connection():
#     conn = mysql.connector.connect(
#         host=os.getenv("DB_HOST"),
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#         database=os.getenv("DB_NAME"),
#         port=os.getenv("DB_PORT"),
#     )
#     return conn




def lambda_handler(event, context):

    host=os.getenv("DB_HOST")
    user=os.getenv("DB_USER")
    password=os.getenv("DB_PASSWORD")
    database=os.getenv("DB_NAME")
    port=os.getenv("DB_PORT")

    eng = create_engine(
        f"mysql+pymysql://{user}:{password}@"
        f"{host}:{port}/{database}"
    )

    client = boto3.client('sqs', region_name='us-east-1')
    FARGATE_SQS_QUEUE_URL = os.getenv("FARGATE_SQS_QUEUE_URL")
    SQS_QUEUE_URL=os.getenv("SQS_QUEUE_URL") 

    query = "SELECT * FROM venue_list WHERE venue_list.venue_status='active';"
    df_venues = pd.read_sql(query, eng)
    print(df_venues.head())
    try:

        for _, row in df_venues.iterrows():
            
            message = {
                "id": row["id"],
                "venue_name": row["venue_name"],
                "venue_url": row["venue_url"],
                "extraction_mode": row["extraction_mode"],
                "venue_status": row["venue_status"],
                "crawler_func_name": row["crawler_func_name"]
            }
            if row["extraction_mode"] == "fargate":
                message["task_def"] = row["crawler_func_name"]
                message["runtime"] = "fargate"
            else:
                message["runtime"] = "lambda"

          

            if message['runtime'] == "fargate":
                response = client.send_message(
                    QueueUrl=FARGATE_SQS_QUEUE_URL,
                    MessageBody=json.dumps(message)
                )
                print(f"Message sent to Fargate crawler SQS: {response['MessageId']}")
            else:
                response = client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(message)
                )
                print(f"Message sent to Lambda crawler SQS: {response['MessageId']}")
            
        return {
            'statusCode': 200
            }
    except Exception as e:
        print(f"Error sending message to SQS: {e}")
        return {
        'statusCode': 500
    }


 
 