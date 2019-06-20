from multiprocessing.pool import Pool
from bs4 import BeautifulSoup
from s3fs import S3FileSystem
import multiprocessing as mp
import boto3

ACCESS_KEY = os.environ['AWS_S3_ACCESS_KEY']
SECRET_KEY = os.environ['AWS_S3_SECRET_ACCESS_KEY']
FS = S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)

def extract_transform_load(path):
    # 1. Read parquet file contents from S3
    # 2. Send to AWS Comprehend
    #     - Get sentiment
    #     - Get entities
    #     - Get key phrases
    #     - Get language
    # 3. Create new pyarrow table
    # 4. Write to new parquet file of the same name into the enriched flder in s3
    pass

def handler(event, context):
    analyzer = boto3.client('comprehend')

    if len(event['Records']) > 0:
        len(events['Records'])

        objects = [(record['s3']['bucket']['name'], record['s3']['object']['key']) for record in event['Records']]
        files = ["s3://{}/{}".format(bucket, key) for bucket, key in objects] 

        pool = Pool(min(mp.cpu_count(), len(files)))
        objects = pool.map(extract_transform_load, files)

        pool.close()
        pool.join()
