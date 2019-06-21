from multiprocessing.pool import Pool
from bs4 import BeautifulSoup
from s3fs import S3FileSystem
import multiprocessing as mp
import pyarrow as pa
import pyarrow.parquet as pq
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
    client = boto3.client('comprehend')
    print('Reading {} from S3'.format(path))
    parquet = pq.read_table(path).to_pydict()
    tweet_text = parquet['tweet_text']
    tweet_language = parquet['tweet_language']
    tweet_entities = []
    tweet_sentiment = []
    tweet_key_phrases = []

    for tweet in tweet_text:
        for lanugage in tweet_language:
            entities = client.detect_entities(Text=tweet,LanguageCode=language)
            sentiment = client.detect_sentiment(Text=tweet,LanguageCode=language)
            key_phrases = client.detect_key_phrases(Text=tweet,LanguageCode=language)
            tweet_entities.append(entities)
            tweet_sentiment.append(sentiment)
            tweet_key_phrases.append(key_phrases)

    enriched_entities = [
        pa.array(tweet_entities),
        pa.array(tweet_sentiment),
        pa.array(tweet_key_phrases)
    ]
    fields = [
        pa.field('entities', pa.struct([('Text', pa.string()),
                                        ('Score', pa.float64()),
                                        ('Type',pa.string()),
                                        ('BeginOffset',pa.int32()),
                                        ('EndOffset',pa.int32())])
        ),
        pa.field('sentiment', pa.string()),
        pa.field('key_phrases', pa.struct([('Score', pa.float64()),
                                           ('Text', pa.string()),
                                           ('BeginOffset',pa.int32()),
                                           ('EndOffset',pa.int32())])
        ),
    ]

    print('Enriching Parquet file with Amazon Comprehend Entities and Sentiment')
    out = path.replace('processed', 'enriched')

    print('Writing to {}'.format(out))
    pq.write_to_dataset(table=table,)

    return True

def handler(event, context):
    status = 'error'

    if len(event['Records']) > 0:
        len(events['Records'])

        objects = [(record['s3']['bucket']['name'], record['s3']['object']['key']) for record in event['Records']]
        files = ["s3://{}/{}".format(bucket, key) for bucket, key in objects]

        pool = Pool(min(mp.cpu_count(), len(files)))
        objects = pool.map(extract_transform_load, files)

        pool.close()
        pool.join()

        status = 'ok'

    return { 'status': status }