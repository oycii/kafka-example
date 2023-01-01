# Пример работы с Kafka

Перед началом выполнения требуется развернуть Kafka через docker-compose (https://github.com/Gorini4/kafka_scala_example) и создать топик books с 3 партициями.
docker-compose up
docker-compose exec broker -ti bash

bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --topic books --partitions 3 \
--replication-factor 1

