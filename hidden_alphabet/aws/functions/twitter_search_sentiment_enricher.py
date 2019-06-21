from hidden_alphabet import transformers as hat
from multiprocessing.pool import Pool
from bs4 import BeautifulSoup
from s3fs import S3FileSystem
import multiprocessing as mp
import pyarrow.parquet as pq
import pyarrow as pa
import boto3
import os

ACCESS_KEY = os.environ['AWS_S3_ACCESS_KEY']
SECRET_KEY = os.environ['AWS_S3_SECRET_ACCESS_KEY']
FS = S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)

def analyzer(row):
    client = boto3.client('comprehend')

    text = str(row[8])
    language = row[11]

    return {
        'user_id': row[0],
        'tweet_id': row[5],
        'conversation_id': row[7],
        'tweet_text': text,
        'entities': client.detect_entities(Text=text, LanguageCode=language),
        'sentiment': client.detect_sentiment(Text=text, LanguageCode=language),
        'key_phrases': client.detect_key_phrases(Text=text, LanguageCode=language)
    }

def extract_transform_load(path):
    print('test')
    parquet = pq.ParquetDataset(path, filesystem=FS).read().to_pydict()
    rows = list(zip(*parquet.values()))
    
    analysis = list(map(analyzer, rows))
    table = hat.utils.list_of_dicts_to_pyarrow_tables(analysis)

    out = path.replace('processed', 'enriched')

    print('Writing to {}'.format(out))
    pq.write_to_dataset(
        table=table,
        root_path=out,
        comprehend='snappy',
        use_dictionary=True,
        filesystem=FS
    )

def handler(event, context):
    status = 'error'

    if len(event.get('Records', [])) > 0:
        objects = [(record['s3']['bucket']['name'], record['s3']['object']['key']) for record in event['Records']]
        files = ["s3://{}/{}".format(bucket, key) for bucket, key in objects]

        pool = Pool(min(mp.cpu_count(), len(files)))
        objects = pool.map(extract_transform_load, files)

        pool.close()
        pool.join()

        status = 'ok'

    return { 'status': status }
