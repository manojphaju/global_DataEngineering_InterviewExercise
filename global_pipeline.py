import sys
import logging
from s3_client import S3Client
from data_processor import DataProcessor
from helpers import load_config, validate_input_date

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    config = load_config()
    access_key_id = config['credentials']['aws_access_key_id']
    secret_access_key = config['credentials']['aws_secret_access_key']

    s3_client = S3Client(access_key_id, secret_access_key)
    data_processor = DataProcessor(s3_client, config)

    if len(sys.argv) != 2:
        print("To run this pipeline use: python3 global_pipeline.py <date>")  # <date> as 2023/08/01
        return

    input_date = sys.argv[1]

    if not validate_input_date(input_date):
        print("Invalid date format. Please use the format: YYYY/MM/DD")
        return

    day_to_process = input_date

    try:
        data_processor.process(day_to_process)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
