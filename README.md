# Test kafka consume

1. run ```docker-compose up -d``` in scripts folder
2. enter container kafka broker. ```docker exec -it <confluentinc/cp-kafka:5.5.1 container id> bash```
3. create topic. ```./usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test-topic```
4. check topic. ```./usr/bin/kafka-topics --list --bootstrap-server localhost:9092```
5. start spring boot server
6. publish message to topic ```./usr/bin/kafka-console-producer --broker-list localhost:9092 --topic test-topic```
