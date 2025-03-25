from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, StreamTableEnvironment
from pyflink.table.udf import TableFunction, udtf

class ParseHastag(TableFunction):
    def eval(self, text: str):
        # Split the text on whitespace and yield tokens that start with "#"
        for token in text.split():
            if token.startswith("#"):
                yield token

def create_posts_source_kafka(t_env):
    table_name = 'post_events'
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        create or replace table {table_name} (
            did varchar,
            time_us bigint,
            kind varchar,
            cid varchar,
            operation varchar,
            created_at varchar,
            text varchar,
            event_timestamp as to_timestamp(created_at, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) with (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'bluesky-raw-posts',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_aggregated_hashtag_counts_sink_postgres(t_env):
    table_name = 'aggregated_hashtag_counts'
    sink_ddl = f"""
        create or replace table {table_name} (
            hashtag varchar,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            hashtag_count BIGINT,
            PRIMARY KEY (window_start, hashtag) NOT ENFORCED
        ) with (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def hashtag_aggregation_job():
     # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    t_env.create_temporary_function("parse_hashtag", udtf(ParseHastag(), result_types=[DataTypes.STRING()]))
    
    try:

        source_table_name = create_posts_source_kafka(t_env)
        source_table = t_env.from_path(source_table_name)

        sink_table_name = create_aggregated_hashtag_counts_sink_postgres(t_env)

        parsed_hashtag_ddl = f""" 
            INSERT INTO {sink_table_name}
            SELECT 
                hashtag,
                window_start, 
                window_end, 
                COUNT(hashtag) AS hashtag_count
            FROM TABLE( 
                TUMBLE( 
                    (SELECT 
                        text, 
                        event_timestamp, 
                        did, 
                        hashtag, 
                        created_at 
                    FROM {source_table}, 
                    LATERAL TABLE(parse_hashtag(coalesce(text, 'empty post'))) AS T(hashtag) ), DESCRIPTOR(event_timestamp), INTERVAL '1' MINUTE ) ) 
                    GROUP BY window_start, window_end, hashtag
        """
        
        t_env.execute_sql(parsed_hashtag_ddl)
        
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))
    
if __name__ == "__main__":
    hashtag_aggregation_job()