import boto3

class S3Client:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def list_objects(self, Bucket, Prefix):
        return self.client.list_objects(Bucket=Bucket, Prefix=Prefix)

    def get_object(self, Bucket, Key):
        return self.client.get_object(Bucket=Bucket, Key=Key)

    def put_object(self, Bucket, Key, Body):
        self.client.put_object(Bucket=Bucket, Key=Key, Body=Body)
