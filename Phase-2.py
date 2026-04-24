import os
import json
import boto3
import paramiko
import pandas as pd
import oracledb
import warnings
from datetime import datetime

# Suppress the Pandas/SQLAlchemy warning
warnings.filterwarnings("ignore", message="Pandas only supports SQLAlchemy")

# Initialize Clients
client = boto3.client('secretsmanager')
s3 = boto3.client('s3')
sns = boto3.client('sns')

def send_sns_notification(subject, message):
    """Sends an email notification via AWS SNS."""
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if not topic_arn:
        print("[WARNING] SNS_TOPIC_ARN not set. Skipping notification.")
        return
    
    try:
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=f"Timestamp: {datetime.now()}\n\n{message}"
        )
        print(f"[LOG] SNS Notification sent: {subject}")
    except Exception as e:
        print(f"[ERROR] Failed to send SNS notification: {e}")


def get_secrets(secret_id):
    """Fetches credentials from AWS Secrets Manager."""
    print(f"[LOG] Fetching secrets for ID: {secret_id}...")
    try:
        response = client.get_secret_value(SecretId=secret_id)
        print("[SUCCESS] Secrets retrieved successfully.")
        return json.loads(response['SecretString'])
    except Exception as e:
        error_msg = f"Failed to fetch secret {secret_id}: {str(e)}"
        print(f"[ERROR] {error_msg}")
        send_sns_notification("CRITICAL: Secrets Fetch Failed", error_msg)
        raise e
    
def upload_s3(file_path, bucket_name, is_output_report=False):
    """Uploads file to S3 and then removes the local copy."""
    now = datetime.now()
    year = now.strftime('%Y')
    month_name = now.strftime('%B-%Y')
    month_year = now.strftime('%d-%m-%Y')
    file_name = os.path.basename(file_path)
    
    prefix = "output/" if is_output_report else "input/"
    s3_key = f"{year}/{month_name}/{month_year}/{prefix}{file_name}"
    
    print(f"[LOG] Uploading to S3: s3://{bucket_name}/{s3_key}")
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"[SUCCESS] Upload complete: {file_name}")
    except Exception as e:
        error_msg = f"S3 Upload failed for {file_name}: {str(e)}"
        print(f"[ERROR] {error_msg}")
        send_sns_notification("PIPELINE ERROR: S3 Upload Failed", error_msg)
        raise e
        
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"[LOG] Cleaned up local file: {file_path}")

def process_database(db_creds, file_path, table_name, ref_query, truncate_query):
    print(f"[LOG] Connecting to Oracle Database...")
    connection = None
    dsn = db_creds['DB_DSN']
    try:
        connection = oracledb.connect(dsn=dsn)
        cursor = connection.cursor()
        print("[SUCCESS] Database connection established.")

        print(f"[LOG] Running reference query: {ref_query}")
        ref_df = pd.read_sql(ref_query, connection)
        
        print(f"[LOG] Reading Excel file into Pandas: {file_path}")
        csv_df = pd.read_excel(file_path, engine='openpyxl', dtype={'ACCOUNT_NUMBER': str})

        print(f"[LOG] Data Transformation: Merging and cleaning...")
        # Subset and clean
        csv_df = csv_df[['ACCOUNT_NUMBER', 'NPADATE_NPA', 'DPD_NPA', 'CLASSIFICATION_NPA']]
        csv_df['ACCOUNT_NUMBER'] = csv_df['ACCOUNT_NUMBER'].astype(str).str.strip()
        ref_df['ACCT_NUM'] = ref_df['ACCT_NUM'].astype(str).str.strip()
        
        merged_df = pd.merge(csv_df, ref_df, left_on='ACCOUNT_NUMBER', right_on='ACCT_NUM', how='left')
        merged_df['NPADATE_NPA'] = pd.to_datetime(merged_df['NPADATE_NPA'], errors='coerce')
        
        mapping = {
            'AOF_NUMBER': 'AOF_NUMBER',
            'ACCOUNT_NUMBER': 'ACCOUNT_NUMBER',
            'CO_LENDING': 'CO_LENDING',
            'DPD_NPA': 'DPD_NPA',
            'CLASSIFICATION_NPA': 'CLASSIFICATION_NPA',
            'NPADATE_NPA': 'NPA_SINCE_DATE'
        }
        merged_df = merged_df.rename(columns=mapping).drop(columns=['ACCT_NUM'], errors='ignore')
        
        print(f"[LOG] Calling truncate procedure: {truncate_query}")
        cursor.callproc(truncate_query)
        
        print(f"[LOG] Preparing bulk insert for {len(merged_df)} rows...")
        columns = list(merged_df.columns)
        placeholders = ', '.join([f":{i+1}" for i in range(len(columns))])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Handle NaT/NaN for Oracle
        data = [tuple(None if pd.isna(val) else val for val in x) for x in merged_df.values]
        
        # Execute the bulk insert query
        cursor.executemany(sql, data)

        # Check for errors after execution
        errors = cursor.getbatcherrors()
        for error in errors:
            print(f"[ERROR] Row {error.offset} failed with error: {error.message}")

        connection.commit()
        print(f"[SUCCESS] Database commit complete for {table_name}.")
        
        return merged_df

    except Exception as e:
        if connection: 
            print("[LOG] Rolling back database transaction due to error.")
            connection.rollback()
        error_msg = f"Database processing error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        send_sns_notification("PIPELINE ERROR: Database Processing Failed", error_msg)
        raise e
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("[LOG] Database connection closed.")

def lambda_handler(event=None, context=None):
    print("--- Pipeline Started ---")
    
    try:
        
        # 1. Fetch Environment Variables
        SECRET_ID = os.environ.get('SECRET_ID')
        TABLE_NAME = os.environ.get('TABLE_NAME')
        TRUNCATE_QUERY = os.environ.get('TRUNCATE_QUERY')
        REMOTE_DIR = os.environ.get('REMOTE_FILE_PATH')
        REF_QUERY = os.environ.get('REF_QUERY')
        FILE_NAME_PREFIX = os.environ.get("FILE_PREFIX")
        BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

        if not all([SECRET_ID, BUCKET_NAME]):
            raise ValueError("Critical environment variables missing (SECRET_ID or S3_BUCKET_NAME)")

        # 2. Get Secrets
        secrets = get_secrets(SECRET_ID)
        
        # 3. Handle SFTP
        today_date = datetime.now().strftime("%d_%m_%y")
        search_pattern = f"{FILE_NAME_PREFIX}_{today_date}" 
        local_temp_folder = "/tmp"
        
        print(f"[LOG] Connecting to SFTP: {secrets['SFTP_HOST']}...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            ssh.connect(
                hostname=secrets['SFTP_HOST'],
                port=int(secrets['SFTP_PORT']),
                username=secrets['SFTP_USER'],
                password=secrets['SFTP_PASS'],
                timeout=30
            )
            
            with ssh.open_sftp() as sftp:
                print(f"[LOG] Searching for file starting with: {search_pattern}")
                files = sftp.listdir(REMOTE_DIR)
                target_file = next((f for f in files if f.startswith(search_pattern) and f.endswith('.xlsx')), None)
                
                if not target_file:
                    print(f"[ERROR] File not found in SFTP directory {REMOTE_DIR}")
                    raise FileNotFoundError(f"SFTP search failed for {search_pattern}")

                local_temp_path = os.path.join(local_temp_folder, target_file)
                print(f"[LOG] Downloading {target_file} to {local_temp_path}")
                sftp.get(f"{REMOTE_DIR}/{target_file}", local_temp_path)
        except Exception as e:
            send_sns_notification("PIPELINE ERROR: SFTP Connection/Transfer Failed", str(e))
            raise e
        finally:
            ssh.close()
            print("[LOG] SFTP connection closed.")

        # 4. Process Database 
        # NOTE: We do this BEFORE uploading to S3, because upload_s3 deletes the local file!
        print("[LOG] Beginning Step 4: Database Processing...")
        final_df = process_database(secrets, local_temp_path, TABLE_NAME, REF_QUERY, TRUNCATE_QUERY)
        
        # 5. Upload INPUT to S3
        print("[LOG] Archiving input file to S3...")
        upload_s3(local_temp_path, BUCKET_NAME, is_output_report=False)

        # 6. Create & Upload OUTPUT Report
        report_file_name = f"upload_report_{today_date}.xlsx"
        report_local_path = f"/tmp/{report_file_name}"
        
        print(f"[LOG] Generating final Excel report: {report_local_path}")
        final_df.to_excel(report_local_path, index=False, engine='openpyxl')
        
        print("[LOG] Archiving output report to S3...")
        upload_s3(report_local_path, BUCKET_NAME, is_output_report=True)

        print("--- Pipeline Finished Successfully ---")
        return {'statusCode': 200, 'body': json.dumps('Pipeline finished successfully!')}

    except Exception as e:
        # Final catch-all for any missed logic
        fatal_error=f"[FATAL] Pipeline failed: {e}"
        send_sns_notification("PIPELINE Failed ERROR: Report Upload/Download", str(fatal_error))
        return {'statusCode': 500, 'body': json.dumps(f'Pipeline failed: {str(e)}')}
