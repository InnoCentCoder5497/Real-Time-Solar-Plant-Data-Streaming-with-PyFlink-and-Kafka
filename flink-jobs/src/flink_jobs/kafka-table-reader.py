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
                        WATERMARK FOR DATE_TIME as DATE_TIME - INTERVAL '30' MINUTE
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
                        WATERMARK FOR DATE_TIME as DATE_TIME - INTERVAL '30' MINUTE
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
    
    env.execute_sql(f"""
                    CREATE TABLE hourly_yield_sink(
                        window_start TIMESTAMP(3),
                        window_end TIMESTAMP(3),
                        PLANT_ID INT,
                        SOURCE_KEY STRING,
                        total_yield DOUBLE
                    ) WITH {get_jdbc_info('solar', 'hourly_yield')}
                """)
    
    
    # Processing and insert to tables
    env.execute_sql("""
                    INSERT INTO daily_yield_sink
                    SELECT
                        PLANT_ID,
                        SOURCE_KEY,
                        TUMBLE_START(DATE_TIME, INTERVAL '1' DAY) AS DATE_TIME,
                        SUM(DAILY_YIELD) AS rolling_sum
                    FROM
                        generator_metrics
                    GROUP BY
                        TUMBLE(DATE_TIME, INTERVAL '1' DAY),
                        PLANT_ID,
                        SOURCE_KEY
                    """
                )
    
    env.execute_sql(f"""
                        INSERT INTO hourly_yield_sink
                        SELECT
                            TUMBLE_START(DATE_TIME, INTERVAL '1' HOUR) AS window_start,
                            TUMBLE_END(DATE_TIME, INTERVAL '1' HOUR) AS window_end,
                            PLANT_ID,
                            SOURCE_KEY,
                            SUM(DAILY_YIELD)  as total_yield
                        FROM 
                            generator_metrics
                        GROUP BY
                            TUMBLE(DATE_TIME, INTERVAL '1' HOUR),
                            PLANT_ID,
                            SOURCE_KEY
                    """)
    
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