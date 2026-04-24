import os
import json
import boto3
import paramiko
import time

# Initialize AWS Clients
s3_client = boto3.client('s3')
sm_client = boto3.client('secretsmanager')
sns_client = boto3.client('sns')

def check_timeout(context, stage, buffer_seconds):
    """
    Checks if remaining time is less than the safety buffer.
    """
    remaining_seconds = context.get_remaining_time_in_millis() / 1000
    print(f"[LOG] Timeout Check at stage '{stage}': {remaining_seconds:.2f}s remaining.")
    
    if remaining_seconds <= buffer_seconds:
        error_msg = f"CRITICAL: Lambda approaching configuration limit at stage: {stage}. Only {remaining_seconds:.2f}s left."
        print(f"[ERROR] {error_msg}")
        
        # Trigger the error SNS for timeout
        sns_client.publish(
            TopicArn=os.environ['ERROR_SNS_TOPIC_ARN'],
            Subject="Lambda Process Interrupted: Time Limit",
            Message=error_msg
        )
        raise Exception(f"Process stopped to prevent hard timeout at stage: {stage}")

def lambda_handler(event, context):
    print("[LOG] Lambda execution started.")
    
    # --- CAPTURE REAL CONFIGURATION ---
    # Capturing the remaining time at the exact start of the handler 
    # gives us the "Total Timeout" set in the AWS console.
    total_configured_time = context.get_remaining_time_in_millis() / 1000
    print(f"[LOG] Real Configuration Detected: Total timeout is set to {total_configured_time}s.")
    
    # Retrieve configurations from Environment Variables
    print("[LOG] Fetching environment variables...")
    SECRET_ID = os.environ['SECRET_ID']
    REMOTE_DIR = os.environ['REMOTE_DIR']
    SUCCESS_SNS_ARN = os.environ['SUCCESS_SNS_TOPIC_ARN']
    ERROR_SNS_ARN = os.environ['ERROR_SNS_TOPIC_ARN']
    
    # We use a safety buffer (default 10s) to stop before the hard limit
    BUFFER = int(os.environ.get('TIMEOUT_BUFFER_SECONDS', 10))
    print(f"[LOG] Safety buffer set to {BUFFER}s. Process will error out if less than {BUFFER}s remains.")
    
    ssh = paramiko.SSHClient()
    local_file_path = ""

    try:
        # 1. Parse S3 Event details
        print("[LOG] Step 1: Parsing S3 event data...")
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        file_name = os.path.basename(key)
        local_file_path = f"/tmp/{file_name}"
        print(f"[LOG] Target File: {key} | Source Bucket: {bucket}")

        check_timeout(context, "S3 Download Start", BUFFER)

        # 2. Download file to /tmp
        print(f"[LOG] Step 2: Downloading {key} to local storage {local_file_path}...")
        s3_client.download_file(bucket, key, local_file_path)
        print(f"[LOG] Successfully downloaded {key}.")

        # 3. Get SFTP Credentials from Secrets Manager
        check_timeout(context, "Secrets Retrieval", BUFFER)
        print(f"[LOG] Step 3: Retrieving secrets from {SECRET_ID}...")
        secret_response = sm_client.get_secret_value(SecretId=SECRET_ID)
        secrets = json.loads(secret_response['SecretString'])
        print("[LOG] Secrets retrieved successfully.")

        # 4. Connect to SFTP
        check_timeout(context, "SFTP Connection", BUFFER)
        print(f"[LOG] Step 4: Connecting to SFTP host: {secrets.get('SFTP_HOST')}...")
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            ssh.connect(
                hostname=secrets['SFTP_HOST'],
                port=int(secrets.get('SFTP_PORT', 1122)),
                username=secrets['SFTP_USER'],
                password=secrets['SFTP_PASS'],
                timeout=15  # Internal socket timeout
            )
            print(f"[LOG] SSH Connection established.")
        except Exception as e:
            print(f"[ERROR] Connection failed: {str(e)}")
            sns_client.publish(
                TopicArn=ERROR_SNS_ARN,
                Subject="SFTP Connection Failure",
                Message=f"Failed to connect to SFTP. Error: {str(e)}"
            )
            raise e

        # 5. Uploading the file
        check_timeout(context, "SFTP Upload", BUFFER)
        print("[LOG] Step 5: Opening SFTP session and verifying directory...")
        with ssh.open_sftp() as sftp:
            try:
                sftp.chdir(REMOTE_DIR)
                print(f"[LOG] Remote directory {REMOTE_DIR} confirmed.")
            except IOError:
                print(f"[ERROR] Path {REMOTE_DIR} not found on server.")
                sns_client.publish(
                    TopicArn=ERROR_SNS_ARN,
                    Subject="SFTP Path Error",
                    Message=f"The path {REMOTE_DIR} does not exist."
                )
                raise Exception(f"Remote directory {REMOTE_DIR} missing.")

            remote_file_name = os.path.basename(local_file_path)    
            remote_full_path = f"{REMOTE_DIR}/{remote_file_name}".replace("//", "/")
            print(f"[LOG] Starting file transfer to {remote_full_path}...")
            
            sftp.put(local_file_path, remote_full_path)
            print(f"[LOG] Upload command completed.")
            
            # --- VERIFICATION ---
            print(f"[LOG] Verifying file exists on SFTP...")
            try:
                sftp.stat(remote_full_path)
                print(f"[LOG] File verified successfully at {remote_full_path}")
            except FileNotFoundError:
                print(f"[ERROR] File missing after upload.")
                raise Exception("Upload verification failed: File not found after put.")
            
        # 6. Success Notification
        print("[LOG] Step 6: Sending success notification...")
        sns_client.publish(
            TopicArn=SUCCESS_SNS_ARN,
            Subject="File Upload Successful",
            Message=f"The file {file_name} was successfully uploaded to {REMOTE_DIR}."
        )
        print("[LOG] Success notification sent.")

    except Exception as e:
        print(f"[ERROR] Exception in Lambda: {str(e)}")
        # Only send general error if it wasn't already handled by specific logic
        if "timeout" not in str(e).lower():
            print("[LOG] Sending general failure notification...")
            sns_client.publish(
                TopicArn=ERROR_SNS_ARN,
                Subject="SFTP Pipeline Failure",
                Message=f"Process failed. Error: {str(e)}"
            )
        raise e

    finally:
        print("[LOG] Finalizing cleanup...")
        ssh.close()
        print("[LOG] SSH connection closed.")
        
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"[LOG] Local file {local_file_path} deleted.")
        else:
            print("[LOG] No local file found to clean up.")
            
        print("[LOG] Execution complete.")
