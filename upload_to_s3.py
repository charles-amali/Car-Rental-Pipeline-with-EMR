import boto3
import os
from dotenv import load_dotenv

def upload_files_to_s3(local_directory, bucket_name, s3_prefix):
    """
    Upload all CSV files from local directory to S3 bucket
    
    Args:
        local_directory (str): Path to local directory containing CSV files
        bucket_name (str): Name of the S3 bucket
        s3_prefix (str): Prefix (folder path) in S3 bucket
    """
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
        
    )
    
    # Ensure s3_prefix doesn't start with '/' and ends with '/'
    s3_prefix = s3_prefix.strip('/')
    if s3_prefix:
        s3_prefix += '/'
    
    # Counter for uploaded files
    uploaded_count = 0
    
    # Walk through the local directory
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            if filename.endswith('.csv'):
                # Construct the full local path
                local_path = os.path.join(root, filename)
                
                # Construct the S3 key (path in S3)
                s3_key = f"{s3_prefix}{filename}"
                
                try:
                    print(f"Uploading {filename} to s3://{bucket_name}/{s3_key}")
                    s3_client.upload_file(local_path, bucket_name, s3_key)
                    uploaded_count += 1
                    print(f"Successfully uploaded {filename}")
                except Exception as e:
                    print(f"Error uploading {filename}: {str(e)}")
    
    print(f"\nUpload complete. {uploaded_count} files uploaded to S3.")

def main():
    # Load environment variables
    load_dotenv()
    
    # S3 bucket configuration
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    if not BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    
    # Local and S3 paths
    LOCAL_DATA_DIR = 'data'
    S3_PREFIX = 'raw'  # This will create a 'raw' folder in your S3 bucket
    
    # Upload files
    upload_files_to_s3(LOCAL_DATA_DIR, BUCKET_NAME, S3_PREFIX)

if __name__ == "__main__":
    main()