"""
AWS Lambda function for polling SQS messages and managing Step Function executions.

This Lambda function acts as a poller that:
1. Monitors an SQS queue for messages
2. Manages concurrency by checking running Step Function executions
3. Batches messages and triggers Step Function executions
4. Ensures messages are only deleted after successful Step Function submission

Environment Variables Required:
- SQS_QUEUE_URL: URL of the SQS queue to poll
- STEP_FUNCTION_ARN: ARN of the Step Function to execute
- BATCH_SIZE (optional): Number of messages to process per batch (default: 10, max: 10)
- MAX_CONCURRENCY (optional): Maximum concurrent Step Function executions (default: 260)
"""

import boto3
import os
import json
from datetime import datetime
import time
from typing import Dict, List, Any

# Initialize AWS service clients
sqs = boto3.client('sqs')
stepfunctions = boto3.client('stepfunctions')

# Configuration from environment variables
QUEUE_URL: str = os.environ['SQS_QUEUE_URL']
STEP_FUNCTION_ARN: str = os.environ['STEP_FUNCTION_ARN']
BATCH_SIZE: int = int(os.environ.get('BATCH_SIZE', 10))  # SQS hard limit is 10 messages per batch
MAX_CONCURRENCY: int = int(os.environ.get('MAX_CONCURRENCY', 260))  # Maximum concurrent Step Function executions

def get_running_execution_count(state_machine_arn):
    paginator = stepfunctions.get_paginator('list_executions')
    running_count = 0
    for page in paginator.paginate(stateMachineArn=state_machine_arn, statusFilter='RUNNING'):
        running_count += len(page['executions'])
    return running_count

def lambda_handler(event, context):
    print("Polling SQS and managing Step Function concurrency...")

    # Add timeout protection
    start_time = time.time()
    max_runtime = 13 * 60  # 13 minutes (leave 2 min buffer for Lambda timeout)

    while True:
        
        # Check if we're approaching Lambda timeout
        if time.time() - start_time > max_runtime:
            print("Approaching Lambda timeout. Exiting gracefully.")
            break


        running_executions = get_running_execution_count(STEP_FUNCTION_ARN)
        print(f"Currently running executions: {running_executions}")

        if running_executions >= MAX_CONCURRENCY:
            print("Max concurrency reached. Exiting.")
            break

        remaining_capacity = MAX_CONCURRENCY - running_executions
        batch_size = min(BATCH_SIZE, remaining_capacity)

        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=1,
            VisibilityTimeout=60
        )

        messages = response.get('Messages', [])
        if not messages:
            print("No more messages in queue.")
            break

        # Process each message individually
        messages_to_delete = []
        
        for msg in messages:
            try:
                parsed_body = json.loads(msg['Body'])
                
                # Generate execution name for individual message
                timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
                event_id = str(parsed_body.get("event_id", "unknown"))
                execution_name = f"lambda-run-{timestamp}-{event_id}"
                
                # Start Step Function for this message
                stepfunctions.start_execution(
                    stateMachineArn=STEP_FUNCTION_ARN,
                    name=execution_name,
                    input=json.dumps([{"body": json.dumps(parsed_body)}])
                )
                print(f"Started Step Function execution: {execution_name}")
                
                # Mark for deletion only after successful submission
                messages_to_delete.append(msg)
                
            except json.JSONDecodeError as e:
                print(f"Invalid JSON in message body: {msg['Body']}")
                # Delete invalid messages to prevent reprocessing
                messages_to_delete.append(msg)
            except Exception as e:
                print(f"Error processing message: {str(e)}")
                # Don't delete on Step Function errors - let message retry
        
        # Delete only successfully processed messages
        for msg in messages_to_delete:
            try:
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg['ReceiptHandle']
                )
            except Exception as e:
                print(f"Error deleting message: {str(e)}")
        
        print(f"Successfully processed {len(messages_to_delete)} messages.")

        # Brief pause to prevent API throttling and allow for graceful processing
        time.sleep(1)