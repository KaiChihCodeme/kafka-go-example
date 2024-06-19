# create topic
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test-topic

# produce msg
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

# consume msg
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning
