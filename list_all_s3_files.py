import boto3
import yaml
from global_pipeline import load_config

def load_config():
    with open('config/config.yaml', 'r') as config_file:
        return yaml.safe_load(config_file)

def list_all_files_in_bucket(bucket_name, access_key, secret_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    response = s3_client.list_objects_v2(Bucket=bucket_name)
    files = [obj['Key'] for obj in response.get('Contents', [])]

    return files

def main():
    config = load_config()
    access_key = config['credentials']['aws_access_key_id']
    secret_key = config['credentials']['aws_secret_access_key']
    bucket_name = 'rd-interview-sample-data'  # Replace with actual bucket name

    files_list = list_all_files_in_bucket(bucket_name, access_key, secret_key)
    print("List of all files in the bucket:")
    for file_key in files_list:
        print(file_key)

if __name__ == "__main__":
    main()
