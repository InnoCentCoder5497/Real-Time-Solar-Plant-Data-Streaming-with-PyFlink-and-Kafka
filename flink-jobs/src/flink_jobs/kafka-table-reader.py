from pyflink.table import TableEnvironment, EnvironmentSettings
import logging
import sys

GENERATOR_TOPIC = 'generator-topic'
WEATHER_TOPIC = 'weather-topic'

def get_jdbc_info(schema_name, table_name):
    return f"""(
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://timescaledb:5432/solar_data',
                'table-name' = '"{schema_name}"."{table_name}"',
                'username' = 'admin',
                'password' = 'admin',
                'driver' = 'org.postgresql.Driver'
            )
    """
    
def get_kafka_info(topic):
    return f"""(
                'connector'= 'kafka',
                'topic'= '{topic}',
                'properties.bootstrap.servers' = 'kafka:29092',
                'format' = 'json',
                'json.timestamp-format.standard' = 'SQL',
                'scan.startup.mode' = 'earliest-offset'
            )
    """

def read_from_kafka(env: TableEnvironment):
    # Source Kafka Reads
    env.execute_sql(f"""
                    CREATE TABLE generator_metrics(
                        DATE_TIME TIMESTAMP(3),
                        DC_POWER DOUBLE,
                        AC_POWER DOUBLE,
                        DAILY_YIELD DOUBLE,
                        TOTAL_YIELD DOUBLE,
                        PLANT_ID INT,
                        SOURCE_KEY STRING,
                        WATERMARK FOR DATE_TIME as DATE_TIME - INTERVAL '30' SECOND
                    )
                    WITH {get_kafka_info(GENERATOR_TOPIC)}
                """    
                )
    
    env.execute_sql(f"""
                    CREATE TABLE weather_metrics(
                        DATE_TIME TIMESTAMP(3),
                        AMBIENT_TEMPERATURE DOUBLE,
                        MODULE_TEMPERATURE DOUBLE,
                        IRRADIATION DOUBLE,
                        PLANT_ID INT,
                        SOURCE_KEY STRING,
                        WATERMARK FOR DATE_TIME as DATE_TIME - INTERVAL '30' SECOND
                    )
                    WITH {get_kafka_info(WEATHER_TOPIC)}
                """    
                )
    
    # Destinations postgres tables
    env.execute_sql(f"""
                    CREATE TABLE daily_yield_sink (
                        PLANT_ID INT,
                        SOURCE_KEY STRING,
                        DATE_TIME TIMESTAMP(3),
                        YIELD DOUBLE,
                        rolling_sum DOUBLE
                    ) WITH {get_jdbc_info('solar', 'daily_yield')}
                    """    
                )

    env.execute_sql(f"""
                    CREATE TABLE generator_metrics_sink (
                        PLANT_ID INT,
                        SOURCE_KEY STRING,
                        DATE_TIME TIMESTAMP(3),
                        DC_POWER DOUBLE,
                        AC_POWER DOUBLE,
                        YIELD DOUBLE
                    ) WITH {get_jdbc_info('solar', 'generator_metrics')}
                """)
    
    env.execute_sql(f"""
                    CREATE TABLE weather_metrics_sink (
                        PLANT_ID INT,
                        SOURCE_KEY STRING,
                        DATE_TIME TIMESTAMP(3),
                        AMBIENT_TEMP DOUBLE,
                        MODULE_TEMP DOUBLE,
                        IRRADIATION DOUBLE
                    ) WITH {get_jdbc_info('solar', 'weather_metrics')}
                """)
    
    
    # Processing and insert to tables
    env.execute_sql("""
                    INSERT INTO daily_yield_sink
                    SELECT
                        PLANT_ID,
                        SOURCE_KEY,
                        DATE_TIME,
                        DAILY_YIELD,
                        SUM(DAILY_YIELD) OVER (
                            PARTITION BY PLANT_ID, SOURCE_KEY
                            ORDER BY DATE_TIME
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS rolling_sum
                    FROM
                        generator_metrics
                    """
                )
    
    env.execute_sql("""
                    INSERT INTO generator_metrics_sink
                    SELECT
                        PLANT_ID,
                        SOURCE_KEY,
                        DATE_TIME,
                        DC_POWER,
                        AC_POWER,
                        DAILY_YIELD
                    FROM
                        generator_metrics
                    """
                )
    
    env.execute_sql("""
                    INSERT INTO weather_metrics_sink
                    SELECT
                        PLANT_ID,
                        SOURCE_KEY,
                        DATE_TIME,
                        AMBIENT_TEMPERATURE,
                        MODULE_TEMPERATURE,
                        IRRADIATION
                    FROM
                        weather_metrics
                    """
                )

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = TableEnvironment.create(
        EnvironmentSettings.in_streaming_mode()
    )
    
    jars = [
        'file:///opt/flink/user-libs/flink-sql-connector-kafka-1.17.2.jar',
        'file:///opt/flink/user-libs/flink-connector-kafka-3.0.1-1.17.jar',
        'file:///opt/flink/user-libs/kafka-clients-3.4.0.jar',
        'file:///opt/flink/user-libs/postgresql-42.7.5.jar',
        'file:///opt/flink/user-libs/flink-connector-jdbc-1.15.0.jar',
    ]
    
    env.get_config().get_configuration().set_string(
        "pipeline.jars",
        ";".join(jars)
    )
    
    logging.info('Starting Read from Kafka')
    
    read_from_kafka(env)