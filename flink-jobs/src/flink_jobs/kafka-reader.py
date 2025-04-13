from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.serialization import DeserializationSchema

import logging
import sys
import os

GENERATOR_TOPIC = 'generator-topic'
WEATHER_TOPIC = 'weather-topic'

def read_from_kafka(env):
    deserialization_schema_generator = JsonRowDeserializationSchema.Builder() \
                            .type_info(
                                Types.ROW_NAMED(
                                    field_names=["DATE_TIME","DC_POWER","AC_POWER","DAILY_YIELD","TOTAL_YIELD", 'PLANT_ID', 'SOURCE_KEY'],
                                    field_types=[Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT(), Types.STRING()]
                                )
                            ).build()
                            
    deserialization_schema_weather = JsonRowDeserializationSchema.Builder() \
                            .type_info(
                                Types.ROW_NAMED(
                                    field_names=["DATE_TIME","AMBIENT_TEMPERATURE","MODULE_TEMPERATURE","IRRADIATION", 'PLANT_ID', 'SOURCE_KEY'],
                                    field_types=[Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT(), Types.STRING()]
                                )
                            ).build()
                            
    kafka_generator_consumer = FlinkKafkaConsumer(
                      topics=GENERATOR_TOPIC,
                      deserialization_schema=deserialization_schema_generator,
                      properties={'bootstrap.servers': 'kafka:29092', 'group.id': 'test_group_1'}
                    )
    
    kafka_weather_consumer = FlinkKafkaConsumer(
                      topics=WEATHER_TOPIC,
                      deserialization_schema=deserialization_schema_weather,
                      properties={'bootstrap.servers': 'kafka:29092', 'group.id': 'test_group_1'}
                    )
    
    kafka_generator_consumer.set_start_from_earliest()
    kafka_weather_consumer.set_start_from_earliest()
    
    env.add_source(kafka_generator_consumer).print()
    env.add_source(kafka_weather_consumer).print()
    env.execute()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    jars = [
        'file:///opt/flink/user-libs/flink-sql-connector-kafka-1.17.2.jar',
        'file:///opt/flink/user-libs/flink-connector-kafka-3.0.1-1.17.jar',
        'file:///opt/flink/user-libs/kafka-clients-3.4.0.jar'
    ]
    env.add_jars(*jars)
    env.set_parallelism(4)
    
    logging.info('Starting Read from Kafka')
    
    read_from_kafka(env)