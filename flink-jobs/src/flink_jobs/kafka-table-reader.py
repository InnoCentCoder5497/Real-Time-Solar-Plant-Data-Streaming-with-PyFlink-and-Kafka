from pyflink.table import TableEnvironment, EnvironmentSettings
import logging
import sys

GENERATOR_TOPIC = 'generator-topic'
WEATHER_TOPIC = 'weather-topic'

def read_from_kafka(env: TableEnvironment):
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
                    WITH (
                        'connector'= 'kafka',
                        'topic'= '{GENERATOR_TOPIC}',
                        'properties.bootstrap.servers' = 'kafka:29092',
                        'format' = 'json',
                        'json.timestamp-format.standard' = 'SQL',
                        'scan.startup.mode' = 'earliest-offset'
                    )
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
                    WITH (
                        'connector'= 'kafka',
                        'topic'= '{WEATHER_TOPIC}',
                        'properties.bootstrap.servers' = 'kafka:29092',
                        'format' = 'json',
                        'json.timestamp-format.standard' = 'SQL',
                        'scan.startup.mode' = 'earliest-offset'
                    )
                """    
                )
    
    env.execute_sql("""
                    CREATE TABLE print_sink (
                        PLANT_ID INT,
                        SOURCE_KEY STRING,
                        DATE_TIME TIMESTAMP(3),
                        DC_POWER DOUBLE,
                        AC_POWER DOUBLE,
                        DAILY_YIELD DOUBLE,
                        TOTAL_YIELD DOUBLE,
                        AMBIENT_TEMPERATURE DOUBLE,
                        MODULE_TEMPERATURE DOUBLE,
                        IRRADIATION DOUBLE
                    ) WITH (
                        'connector' = 'print'
                    )
                """)
    
    env.execute_sql("""
                    INSERT INTO print_sink
                    SELECT
                        g.PLANT_ID,
                        g.SOURCE_KEY,
                        w.DATE_TIME,
                        g.DC_POWER,
                        g.AC_POWER,
                        g.DAILY_YIELD,
                        g.TOTAL_YIELD,
                        w.AMBIENT_TEMPERATURE,
                        w.MODULE_TEMPERATURE,
                        w.IRRADIATION
                    FROM
                        generator_metrics AS g
                    JOIN
                        weather_metrics AS w
                    ON
                        g.PLANT_ID = w.PLANT_ID AND w.DATE_TIME BETWEEN g.DATE_TIME - INTERVAL '30' SECONDS AND g.DATE_TIME + INTERVAL '30' SECONDS;
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
        'file:///opt/flink/user-libs/kafka-clients-3.4.0.jar'
    ]
    
    env.get_config().get_configuration().set_string(
        "pipeline.jars",
        ";".join(jars)
    )
    # env.add_jars(*jars)
    # env.set_parallelism(4)
    
    logging.info('Starting Read from Kafka')
    
    read_from_kafka(env)