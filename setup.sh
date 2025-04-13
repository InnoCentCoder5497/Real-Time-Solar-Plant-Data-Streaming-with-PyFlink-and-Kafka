echo "Creating Generator Topic"
docker exec kafka ./opt/bitnami/kafka/bin/kafka-topics.sh --create --topic generator-topic --bootstrap-server localhost:9092
echo "Creating Weather Topic"
docker exec kafka ./opt/bitnami/kafka/bin/kafka-topics.sh --create --topic weather-topic --bootstrap-server localhost:9092

echo "Topic Setup complete"

echo "Starting Flink Job"
docker exec jobmanager flink run --python /opt/flink/jobs/kafka-reader.py