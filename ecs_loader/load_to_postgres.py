import psycopg2, boto3, json, requests, re, logging, io
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import pandas as pd
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2 import pool
from datetime import datetime
from typing import Union, Dict


WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/XXXX/messages?key=XXXX&token=XXXX"

S3_PATH = "s3://data-pipeline-example/emr_output/etl_jobs/"
MAX_CONNECTIONS = 5
THREADS = 5
logging.basicConfig(level=logging.INFO)

class DataPipeline:
    """
    Data Pipeline class to orchestrate:
    1. Fetching credentials from AWS SSM
    2. Creating staging tables in PostgreSQL
    3. Loading CSV files from S3 into PostgreSQL
    4. Creating or refreshing views
    5. Cleaning old data and temporary files
    """

    def __init__(self, env: str, parameter_name: str):
        self.env = env
        self.parameter_name = parameter_name

    # ------------------------------------------------------------------
    # Google Chat Messaging
    # ------------------------------------------------------------------
    @staticmethod
    def send_chat_message(webhook_url: str, content: Union[str, Dict], message_type: str = "text") -> bool:
        """
        Send notification message to Google Chat.
        """
        headers = {"Content-Type": "application/json; charset=UTF-8"}
        try:
            if message_type == "card" and isinstance(content, dict):
                payload = content
            elif message_type == "text":
                payload = {"text": content}
            else:
                raise ValueError("Invalid message type or content format.")

            response = requests.post(webhook_url, headers=headers, data=json.dumps(payload), timeout=10)
            response.raise_for_status()
            logging.info("Message sent successfully!")
            return True
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            return False

    @staticmethod
    def error_card(title: str, error_msg: str = "") -> Dict:
        """
        Build a Google Chat card with error details.
        """
        return {
            "cards": [{
                "header": {"title": "🚨 Pipeline Error", "subtitle": title},
                "sections": [{
                    "widgets": [{
                        "textParagraph": {
                            "text": f"Details: {error_msg}" if error_msg else "Check logs for more information."
                        }
                    }]
                }]
            }]
        }

    @staticmethod
    def success_card(message: str) -> Dict:
        """
        Build a Google Chat card for successful execution.
        """
        return {"cards": [{"header": {"title": "✅ Pipeline Info", "subtitle": message}}]}

    # ------------------------------------------------------------------
    # AWS & Database Connections
    # ------------------------------------------------------------------
    @staticmethod
    def get_parameter(parameter_name: str, region_name: str = "us-east-1") -> Dict:
        """
        Fetch and decrypt credentials stored in AWS SSM Parameter Store.
        """
        try:
            ssm_client = boto3.client("ssm", region_name=region_name)
            response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
            return json.loads(response["Parameter"]["Value"])
        except Exception as e:
            logging.error(f"Error fetching parameter {parameter_name}: {e}")
            raise

    @staticmethod
    def connect_to_db(host: str, user: str, password: str, dbname: str = "analytics") -> psycopg2.extensions.connection:
        """
        Establish a PostgreSQL connection.
        """
        try:
            conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
            logging.info("Connected to database successfully.")
            return conn
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise

    # ------------------------------------------------------------------
    # Schema / Table Management
    # ------------------------------------------------------------------
    def create_table(self, schema: str, table: str, date: str):
        """
        Create a staging table with partitioned date suffix and indexes.
        """
        creds = self.get_parameter(self.parameter_name)
        with self.connect_to_db(creds["host"], creds["user"], creds["password"]) as conn:
            with conn.cursor() as cursor:
                q_table = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table}_{date} (
                        region_id INT,
                        season_id INT,
                        crop_id INT,
                        area FLOAT
                    );
                """
                cursor.execute(q_table)

                q_index = f"""
                    CREATE INDEX IF NOT EXISTS idx_{schema}_{table}_{date}_region
                        ON {schema}.{table}_{date} (region_id);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_{schema}_{table}_{date}_unique
                        ON {schema}.{table}_{date} (region_id, season_id, crop_id);
                """
                cursor.execute(q_index)
                conn.commit()
                logging.info("Table %s.%s_%s created with indexes.", schema, table, date)

    # ------------------------------------------------------------------
    # S3 Utilities
    # ------------------------------------------------------------------
    @staticmethod
    def list_csv_files(s3_client, bucket: str, prefix: str) -> list:
        """
        List all CSV files from an S3 bucket path.
        """
        csv_files, continuation_token = [], None
        while True:
            args = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                args["ContinuationToken"] = continuation_token
            response = s3_client.list_objects_v2(**args)

            contents = response.get("Contents", [])
            csv_files.extend([obj["Key"] for obj in contents if obj["Key"].endswith(".csv")])

            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break
        return csv_files

    # ------------------------------------------------------------------
    # Data Loading
    # ------------------------------------------------------------------
    @staticmethod
    def upload_csv_to_postgres(s3_client, bucket: str, key: str, connection_pool, schema: str, table: str, date: str):
        """
        Load a single CSV file from S3 into PostgreSQL using COPY.
        """
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(io.BytesIO(obj["Body"].read()), header=None)

            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            conn = connection_pool.getconn()
            with conn.cursor() as cursor:
                copy_sql = f"""
                    COPY {schema}.{table}_{date} (region_id, season_id, crop_id, area)
                    FROM STDIN WITH (FORMAT CSV)
                """
                cursor.copy_expert(copy_sql, buffer)
            conn.commit()
            connection_pool.putconn(conn)
            logging.info("Loaded %s", key)
        except Exception as e:
            logging.error("Error loading %s: %s", key, e)
            raise

    def load_data(self, s3_path: str, schema: str, table: str, date: str):
        """
        Orchestrate CSV extraction from S3 and load into PostgreSQL.
        """
        s3 = boto3.client("s3")
        parsed = urlparse(s3_path)
        bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
        csv_files = self.list_csv_files(s3, bucket, prefix)

        if not csv_files:
            logging.warning("No CSV files found at %s", s3_path)
            return

        creds = self.get_parameter(self.parameter_name)
        connection_pool = pool.SimpleConnectionPool(
            minconn=1, maxconn=MAX_CONNECTIONS,
            host=creds["host"], user=creds["user"],
            password=creds["password"], dbname="analytics", port=5432
        )

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {
                executor.submit(self.upload_csv_to_postgres, s3, bucket, key,
                                connection_pool, schema, table, date): key
                for key in csv_files
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.send_chat_message(WEBHOOK_URL,
                        self.error_card(f"Error loading {schema}.{table}_{date}", str(e)), "card")
                    raise
        connection_pool.closeall()
        logging.info("All files loaded successfully.")

    # ------------------------------------------------------------------
    # View and Cleanup
    # ------------------------------------------------------------------
    def create_or_replace_view(self, schema: str, table: str, date: str):
        """
        Create or refresh a view pointing to the latest partitioned table.
        """
        creds = self.get_parameter(self.parameter_name)
        with self.connect_to_db(creds["host"], creds["user"], creds["password"]) as conn:
            with conn.cursor() as cursor:
                query = f"""
                    CREATE OR REPLACE VIEW {schema}.vw_{table} AS
                    SELECT * FROM {schema}.{table}_{date};
                """
                cursor.execute(query)
                conn.commit()
                logging.info("View %s.vw_%s refreshed.", schema, table)

    @staticmethod
    def delete_s3_files(s3_path: str):
        """
        Delete all files from the specified S3 path.
        """
        parsed = urlparse(s3_path)
        bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")

        to_delete = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                to_delete.append({"Key": obj["Key"]})

        if to_delete:
            for i in range(0, len(to_delete), 1000):
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete[i:i+1000]})
            logging.info("Deleted %d files from %s/%s", len(to_delete), bucket, prefix)

    def delete_old_table(self, schema: str, table: str):
        """
        Drop the oldest partitioned table if multiple exist.
        """
        creds = self.get_parameter(self.parameter_name)
        with self.connect_to_db(creds["host"], creds["user"], creds["password"]) as conn:
            with conn.cursor() as cursor:
                q_tables = f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name LIKE '{table}%';
                """
                cursor.execute(q_tables)
                tables = [row[0] for row in cursor.fetchall()]

                if len(tables) >= 2:
                    dates = [datetime.strptime(''.join(re.findall(r'\d', t)), "%Y%m%d") for t in tables]
                    old_date = min(dates).strftime("%Y_%m_%d")
                    q_drop = f"DROP TABLE {schema}.{table}_{old_date}"
                    cursor.execute(q_drop)
                    conn.commit()
                    logging.info("Dropped old table: %s.%s_%s", schema, table, old_date)

if __name__ == "__main__":
    env = "prod"
    parameter_name = f"/myproject/{env}/db/credentials"

    pipeline = DataPipeline(env, parameter_name)
    date_str = datetime.now().strftime("%Y_%m_%d")

    pipeline.create_table("staging", "crop_analytics", date_str)
    pipeline.load_data(S3_PATH, "staging", "crop_analytics", date_str)
    pipeline.create_or_replace_view("staging", "crop_analytics", date_str)
    pipeline.delete_s3_files(S3_PATH)
    pipeline.delete_old_table("staging", "crop_analytics")
