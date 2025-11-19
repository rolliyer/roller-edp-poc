from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from data_loader import load_jsonl_gz_to_bigquery

from google.cloud import storage
from google.cloud import bigquery

# --- Configuration Variables ---
API_BASE_URL = "https://apim.workato.com"  # Base URL configured in HTTP connection
API_ENDPOINT = "/roller/sfdc-bulk-extract-v1/extractRecords"
API_TOKEN_CONN_ID = "" # Connection ID storing the API Token

# --- Define the Request Body (Payload) ---
# This payload triggers your Workato bulk extract recipe
ACC_REQUEST_PAYLOAD = {
    "sfdc_object_type": "Account",
    "record_limit": 5000
}

LEAD_REQUEST_PAYLOAD = {
    "sfdc_object_type": "Lead",
    "record_limit": 5000
}

OPP_REQUEST_PAYLOAD = {
    "sfdc_object_type": "Opportunity",
    "record_limit": 5000
}

CONTACT_REQUEST_PAYLOAD = {
    "sfdc_object_type": "Contact",
    "record_limit": 5000
}

USER_REQUEST_PAYLOAD = {
    "sfdc_object_type": "User",
    "record_limit": 5000
}


with DAG(
    dag_id="workato_sfdc_bulk_extract_trigger",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["http", "workato", "etl"],
) as dag:
    
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    # --- 3. The HttpOperator Task ---
    account_extract = HttpOperator(
        task_id="account_extract",
        http_conn_id="workato_http_base",  # Base URL defined here
        endpoint=API_ENDPOINT,      # Specific path component

        # HTTP Method: POST for triggering processes with a body
        method="GET",

        # Headers: Essential for API Token authentication
        # The 'Authorization' value will be pulled from the 'workato_api_conn' secret
        headers={
            "Content-Type": "application/json",
            "api-token": API_TOKEN_CONN_ID  
        },

        # Data: The JSON payload you want to send
        data=ACC_REQUEST_PAYLOAD,
        
        # Ensures the data is converted to JSON string format
        log_response=True,
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: print(response.text)
    )

    lead_extract = HttpOperator(
        task_id="lead_extract",
        http_conn_id="workato_http_base",  # Base URL defined here
        endpoint=API_ENDPOINT,      # Specific path component

        # HTTP Method: POST for triggering processes with a body
        method="GET",

        # Headers: Essential for API Token authentication
        # The 'Authorization' value will be pulled from the 'workato_api_conn' secret
        headers={
            "Content-Type": "application/json",
            "api-token": API_TOKEN_CONN_ID  
        },

        # Data: The JSON payload you want to send
        data=LEAD_REQUEST_PAYLOAD,
        
        # Ensures the data is converted to JSON string format
        log_response=True,
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: print(response.text)
    )

    opportunity_extract = HttpOperator(
        task_id="opportunity_extract",
        http_conn_id="workato_http_base",  # Base URL defined here
        endpoint=API_ENDPOINT,      # Specific path component

        # HTTP Method: POST for triggering processes with a body
        method="GET",

        # Headers: Essential for API Token authentication
        # The 'Authorization' value will be pulled from the 'workato_api_conn' secret
        headers={
            "Content-Type": "application/json",
            "api-token": API_TOKEN_CONN_ID  
        },

        # Data: The JSON payload you want to send
        data=OPP_REQUEST_PAYLOAD,
        
        # Ensures the data is converted to JSON string format
        log_response=True,
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: print(response.text)
    )

    contact_extract = HttpOperator(
        task_id="contact_extract",
        http_conn_id="workato_http_base",  # Base URL defined here
        endpoint=API_ENDPOINT,      # Specific path component

        # HTTP Method: POST for triggering processes with a body
        method="GET",

        # Headers: Essential for API Token authentication
        # The 'Authorization' value will be pulled from the 'workato_api_conn' secret
        headers={
            "Content-Type": "application/json",
            "api-token": API_TOKEN_CONN_ID  
        },

        # Data: The JSON payload you want to send
        data=CONTACT_REQUEST_PAYLOAD,
        
        # Ensures the data is converted to JSON string format
        log_response=True,
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: print(response.text)
    )

    user_extract = HttpOperator(
        task_id="user_extract",
        http_conn_id="workato_http_base",  # Base URL defined here
        endpoint=API_ENDPOINT,      # Specific path component

        # HTTP Method: POST for triggering processes with a body
        method="GET",

        # Headers: Essential for API Token authentication
        # The 'Authorization' value will be pulled from the 'workato_api_conn' secret
        headers={
            "Content-Type": "application/json",
            "api-token": API_TOKEN_CONN_ID  
        },

        # Data: The JSON payload you want to send
        data=USER_REQUEST_PAYLOAD,
        
        # Ensures the data is converted to JSON string format
        log_response=True,
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: print(response.text)
    )

    required_columns = [
    "Id",
    "Name",
    "Industry",
    "createdDate"
    ]

    bq_schema = [
        bigquery.SchemaField("Id", "STRING"),
        bigquery.SchemaField("Name", "STRING"),
        bigquery.SchemaField("Industry", "STRING"),
        bigquery.SchemaField("createdDate", "TIMESTAMP")
    ]

    load_account = PythonOperator(
            task_id='load_account_bq',
            python_callable=load_jsonl_gz_to_bigquery,
            op_kwargs={ "gcs_bucket": "raw_edp_salesforce",
                        "gcs_blob_path": "source=salesforce/object_type=Account/year=2025/month=11/day=19/hour=11/214062_20251119T114908Z.jsonl.gz",
                        "project_id": "ent-data-warehouse-dev",
                        "dataset_id": "edp_bronze",
                        "table_id": "python_account",
                        "required_columns": required_columns,
                        "bq_schema": bq_schema
                    }
        )

    start_task >> account_extract >> load_account >> lead_extract >> opportunity_extract >> contact_extract >> user_extract >> end_task