import os
import json
from app import lambda_function

def main():
    payload = os.getenv("PAYLOAD", "{}")
    print("PAYLOAD:", payload)
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        raise ValueError("Invalid PAYLOAD format")
     # Enclose event in {"parsed": event}
    event = {"parsed": event}
    print("Running in Fargate mode...")
    response = lambda_function.lambda_handler(event, context=None)
    print("Lambda-style response:", response)

if __name__ == "__main__":

    main()
