
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.decorators import task
from clickhouse_driver import Client


client = Client(
    host='clickhouse',
    port=9000,
    user='default',
    password='ckdba.123',
)

with DAG(dag_id='taobao_user_purchases_example',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1),
         tags=['clickhouse', 'airbyte'],
         ) as dag:

    user_info_to_clickhouse = AirbyteTriggerSyncOperator(
        task_id='user_info_to_clickhouse',
        airbyte_conn_id='airbyte',
        connection_id='dd5ebac2-2827-460a-bc5b-d96b68eb5e8a',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    user_purchases_to_clickhouse = AirbyteTriggerSyncOperator(
        task_id='user_purchases_to_clickhouse',
        airbyte_conn_id='airbyte',
        connection_id='1f37ca26-b381-4fb9-8b68-fa1e633cc505',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    @task
    def clickhouse_upsert_userinfo():
        client.execute("""
        -- Drop the old table if it exists
        DROP TABLE IF EXISTS default.user_info;
                       """
                       )

        client.execute("""
        -- Define the user_info table structure
        CREATE TABLE default.user_info
        (
            user_id   String,
            gender    String,
            birthday  Date
        )
            ENGINE = MergeTree
            ORDER BY user_id;
                       """
                       )

        client.execute("""
        -- Insert data into the new table from the JSON, adjusting the gender field
        INSERT INTO default.user_info
        SELECT JSONExtractString(_airbyte_data, 'user_id') AS user_id,
            CASE JSONExtractInt(_airbyte_data, 'gender')
                WHEN 0 THEN 'Male'  -- Boy
                WHEN 1 THEN 'Female'  -- Girl
                ELSE 'Unknown'  -- Unknown gender
                END AS gender,
            parseDateTimeBestEffortOrNull(nullIf(JSONExtractString(_airbyte_data, 'birthday'), '')) AS birthday
        FROM airbyte_internal.default_raw__stream_tianchi_mum_baby;
                       """
                       )

    @task
    def clickhouse_upsert_user_purchases():
        client.execute("""
        -- Drop the old table if it exists
        DROP TABLE IF EXISTS default.user_purchases;
                        """
                       )

        client.execute("""
        -- Define the new table structure
        CREATE TABLE default.user_purchases
        (
            user_id     String,
            auction_id  String,
            buy_mount   Int32,
            day         Date,
            category_1  String,
            category_2  String
        )
            ENGINE = MergeTree
            ORDER BY user_id;
                        """
                       )

        client.execute("""
        -- Insert data into the new table from the JSON
        INSERT INTO default.user_purchases
        SELECT JSONExtractString(_airbyte_data, 'user_id') AS user_id,
            JSONExtractString(_airbyte_data, 'auction_id') AS auction_id,
            JSONExtractInt(_airbyte_data, 'buy_mount') AS buy_mount,
            parseDateTimeBestEffortOrNull(nullIf(JSONExtractString(_airbyte_data, 'day'), '')) AS day,
            JSONExtractString(_airbyte_data, 'category_1') AS category_1,
            JSONExtractString(_airbyte_data, 'category_2') AS category_2
        FROM airbyte_internal.default_raw__stream_tianchi_mum_baby_trade_history;
                       """
                       )

    @task
    def clickhouse_upsert_dws_user_purchases():
        client.execute("""
        -- Drop the old wide table if it exists
        DROP TABLE IF EXISTS default.dws_user_purchases;
                       """
                       )

        client.execute("""
        -- Define the new wide table structure
        CREATE TABLE default.dws_user_purchases
        (
            user_id      String,
            auction_id   String,
            buy_mount    Int32,
            day          Date,
            category_1   String,
            category_2   String,
            gender       String,
            birthday     Date
        )
            ENGINE = MergeTree
            ORDER BY user_id;
                       """
                       )

        client.execute("""
        -- Insert data into the new wide table by left joining user_purchases and user_info
        INSERT INTO default.dws_user_purchases
        SELECT
            up.user_id,
            up.auction_id,
            up.buy_mount,
            up.day,
            up.category_1,
            up.category_2,
            ui.gender,
            ui.birthday
        FROM default.user_purchases AS up
        LEFT JOIN default.user_info AS ui
        ON up.user_id = ui.user_id;
                       """
                       )

    # Define dependencies
    user_info_to_clickhouse >> clickhouse_upsert_userinfo()
    user_purchases_to_clickhouse >> clickhouse_upsert_user_purchases()
    [clickhouse_upsert_userinfo(), clickhouse_upsert_user_purchases()] >> clickhouse_upsert_dws_user_purchases()
