from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema


import logging
import sys
import os

def read_from_kafka(env):
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
                            .type_info(
                                Types.ROW_NAMED(
                                    field_names=["DATE_TIME","DC_POWER","AC_POWER","DAILY_YIELD","TOTAL_YIELD"],
                                    field_types=[Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()]
                                )
                            ).build()
                            
    kafka_consumer = FlinkKafkaConsumer(
                      topics='generator-topic',
                      deserialization_schema=deserialization_schema,
                      properties={'bootstrap.servers': 'kafka:29092', 'group.id': 'test_group_1'}
                    )
    
    kafka_consumer.set_start_from_earliest()
    env.add_source(kafka_consumer).print()
    env.execute()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    print(os.getcwd())
    env = StreamExecutionEnvironment.get_execution_environment()
    jars = [
        'file:///opt/flink/user-libs/flink-sql-connector-kafka-1.17.2.jar',
        'file:///opt/flink/user-libs/flink-connector-kafka-3.0.1-1.17.jar',
        'file:///opt/flink/user-libs/kafka-clients-3.4.0.jar'
    ]
    env.add_jars(*jars)
    
    logging.info('Starting Read from Kafka')
    
    read_from_kafka(env)