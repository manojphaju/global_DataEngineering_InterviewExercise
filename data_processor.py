import logging
import io
import pandas as pd

class DataProcessor:
    def __init__(self, s3_client, config):
        self.s3_client = s3_client
        self.config = config

    def process(self, day_to_process):
        source_bucket = 'rd-interview-sample-data'
        source_prefix = f'raw/{day_to_process}/'
        response = self.s3_client.list_objects(Bucket=source_bucket, Prefix=source_prefix)
        files_to_process = [obj['Key'] for obj in response.get('Contents', [])]

        for file_key in files_to_process:
            try:
                # Read file directly from S3
                s3_object = self.s3_client.get_object(Bucket=source_bucket, Key=file_key)
                content = s3_object['Body'].read()

                df = pd.read_csv(io.BytesIO(content))

                # Data processing logic here
                df = df.drop_duplicates(subset=['IMPRESSION_ID', 'IMPRESSION_DATETIME'])

                # Aggregation by CAMPAIGN_ID and hour
                df['IMPRESSION_DATETIME'] = pd.to_datetime(df['IMPRESSION_DATETIME'])
                df['Hour'] = df['IMPRESSION_DATETIME'].dt.hour
                df_aggregated = df.groupby(['CAMPAIGN_ID', 'Hour']).size().reset_index(name='COUNT')

                # Convert DataFrame to CSV content
                target_content = df_aggregated.to_csv(index=False)

                # Write aggregated data to S3
                target_bucket = 'rd-interview-sample-data'
                target_prefix = f'results/{day_to_process.replace("/", "")}/'
                target_file_key = f'daily_agg_{day_to_process.replace("/", "")}_{self.config["initials"]}.csv'

                self.s3_client.put_object(Bucket=target_bucket, Key=f'{target_prefix}{target_file_key}', Body=target_content)
            except Exception as e:
                logging.error(f"An error occurred while processing {file_key}: {e}")

        logging.info("Data processing completed")
