```bash

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic test-topic --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server localhost:9092 --list
kafka-topics --bootstrap-server=localhost:9092 --describe --topic test-topic


docker exec fastapi-app nc -zv kafka 29092

nc -zv localhost 9092

docker logs kafka | grep -i "mode"


docker exec -it kafka bash
docker exec -it 82fdb31d2d83  bash

in bash:
    kafka-console-consumer --bootstrap-server :9092 --topic test-topic --from-beginning



```
