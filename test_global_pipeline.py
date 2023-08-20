import pytest
import datetime
from unittest.mock import MagicMock
from s3_client import S3Client
from data_processor import DataProcessor
from helpers import load_config

class MockS3Client:
    def __init__(self):
        self.objects = {}

    def list_objects(self, Bucket, Prefix):
        test_date = datetime.date(2021, 1, 30)  # Construct a valid date
        formatted_date = test_date.strftime('%Y/%m/%d')  # Convert to desired format
        return {'Contents': [{'Key': f'raw/{formatted_date}/20210130.csv'}]}

    def get_object(self, Bucket, Key):
        content = b"IMPRESSION_ID,IMPRESSION_DATETIME,CAMPAIGN_ID\n1,2021-01-30 00:00:00,1\n2,2021-01-30 01:00:00,1\n"
        return {'Body': MagicMock(read=lambda: content)}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

@pytest.fixture
def mock_s3_client():
    return MockS3Client()

def test_data_processor_process(mock_s3_client: MockS3Client):
    config = load_config()
    s3_client = S3Client(config['credentials']['aws_access_key_id'], config['credentials']['aws_secret_access_key'])  # Replace with actual keys
    data_processor = DataProcessor(s3_client, config)

    test_day_to_process = '2021/01/30'

    # Call the process method and test assertions
    data_processor.process(test_day_to_process)

    # Debugging print statements
    print("Contents of mock_s3_client.objects:", mock_s3_client.objects)

    expected_key = f'results/{test_day_to_process.replace("/", "")}/daily_agg_{test_day_to_process.replace("/", "")}_{config["initials"]}.csv'
    print("Expected key:", expected_key)

    # Perform assertions based on expected results
    assert expected_key in mock_s3_client.objects
