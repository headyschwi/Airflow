import sys
sys.path.append("app_airflow")


from airflow.models import DAG, BaseOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
import pathlib
import json
import requests

class TwitterHook(HttpHook):
    def __init__(self, search_query, start_time, end_time, conn_id = 'twitter_default'):
        super().__init__(http_conn_id = conn_id)
        self.search_query = search_query
        self.start_time = start_time
        self.end_time = end_time
        self.conn_id = conn_id

    def create_url(self):

        TIMESTAMP = '%Y-%m-%dT%H:%M:%S.00Z'
        
        tweet_fields = 'tweet.fields=author_id,created_at,lang,public_metrics,source,text,withheld'
        user_fields = 'expansions=author_id&user.fields=created_at,description,location,name,public_metrics,username,verified,withheld'

        url_raw = f"{self.base_url}/tweets/search/recent?query={self.search_query}&{tweet_fields}&{user_fields}&start_time={self.start_time}&end_time={self.end_time}"

        return url_raw

    def connect_to_endpoint(self, url, session):
        request = requests.Request('GET', url)
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")

        return self.run_and_check(session, prep, {})  


    def paginate(self, url, session):

        responses = []
        response = self.connect_to_endpoint(url, session)
        json_response = response.json()

        responses.append(json_response)

        while 'next_token' in json_response.get('meta', {}):
            next_token = json_response['meta']['next_token']

            url_next = f"{url}&next_token={next_token}"
            json_response = self.connect_to_endpoint(url_next, session).json()

            responses.append(json_response)
        
        return responses
    
    def run(self):
        session = self.get_conn()
        url = self.create_url()

        return self.paginate(url, session)
    
class TwitterOperator(BaseOperator):

    template_fields = ['file_path', 'search_query', 'start_time', 'end_time']

    def __init__(self, file_path, search_query, start_time, end_time, conn_id = 'twitter_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_query = search_query
        self.start_time = start_time
        self.end_time = end_time
        self.conn_id = conn_id
        self.file_path = file_path
    
    def execute(self, context):

        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

        with open(self.file_path, "w") as f:
            hook = TwitterHook(self.search_query, self.start_time, self.end_time, self.conn_id)
            response = hook.run()
            for page in response:
                json.dump(page, f, ensure_ascii=False)
                f.write("\n")
            return response

with DAG(dag_id='TwitterDAG', start_date=days_ago(7), schedule_interval="@daily") as dag:
    
    BASE_FOLDER = join(str(Path("~/Desktop").expanduser()), 
                        "Airflow/Twitter/data/{stage}/{extract_date_start}")
    
    EXTRACT_DATE_START = "{{ data_interval_start.strftime('%Y-%m-%d') }}"

    query = 'data science'

    twitter_operator = TwitterOperator(
        task_id='get_raw_data_twitter',        
        file_path=join(BASE_FOLDER.format(stage="raw", extract_date_start=EXTRACT_DATE_START), " {{ ds }}.json"), 
        search_query = query, 
        start_time = "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
        end_time = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}"
    )
        
    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_data",
        application="/home/eder/Desktop/Airflow/Twitter/src/spark/transformation.py",
        name="Twitter Transformation",
        application_args=["--src", join(BASE_FOLDER.format(stage="raw", extract_date_start = EXTRACT_DATE_START)),
                          "--dest", join(BASE_FOLDER.format(stage="processed", extract_date_start = "")),
                          "--process_date", "{{ ds }}" 
                          ]
    )

    twitter_insights = SparkSubmitOperator(
        task_id="insights_twitter_data",
        application="/home/eder/Desktop/Airflow/Twitter/src/spark/insights_twitter.py",
        name="Twitter Insights",
        application_args=["--src", join(BASE_FOLDER.format(stage="processed", extract_date_start="")),
                          "--dest", join(BASE_FOLDER.format(stage="final", extract_date_start="")),
                          "--process_date", "{{ ds }}"
                          ]      
    )

    twitter_operator >> twitter_transform >> twitter_insights