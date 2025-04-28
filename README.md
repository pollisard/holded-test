# Holded Data Engineer Challenge

## Requirements
- Docker Desktop
- poetry or another virtualenv

## Build and Up the platform
Execute the following command
```
$ cd de-sde-challenge
$ docker compose up -d --build
$ docker ps -a
```
You should have the following containers up and running

```
IMAGE                                    PORTS                                            NAMES
provectuslabs/kafka-ui:latest            0.0.0.0:8080->8080/tcp                           kafka-ui
acme-kafka-connect                       0.0.0.0:8083->8083/tcp, 9092/tcp                 kafka-connect0
confluentinc/cp-schema-registry:7.6.1    8081/tcp, 0.0.0.0:8085->8085/tcp                 schema-registry0
dpage/pgadmin4:latest                    443/tcp, 0.0.0.0:5050->80/tcp                    pgadmin
acme-kafka-connect                                                                        kafka-connect-builder
confluentinc/cp-kafka:7.6.1              0.0.0.0:9092->9092/tcp, 0.0.0.0:9997->9997/tcp   kafka0
minio/minio                              0.0.0.0:9000-9001->9000-9001/tcp                 minio-storage
postgres:15                              0.0.0.0:5432->5432/tcp                           postgres
```

**Note 1**: Remember to unzip the `events.json.zip` file.
**Note 2**: You can use `Avro Schema` or `Protobuf` for your schema definitions

## Tasks TODO

- [x] Implement the HTTP POST /collect method in `api/api/collect.py`
    - [x] Connect to Schema Registry
    - [x] Validate and serialize events in JSON
    - [x] Send valid event to Kafka `events.raw`
    - [x] Send invalid events to Kafka `events.dead.letter`
- [x] Implement Apache Beam Streaming application to read from Kafka
    - [x] Deserialize and validate the JSON using Schema Registry
    - [x] Transform the event to include the new `metadata` field
    - [ ] Send results to minio and postgresql.
- [ ] Create schema for `events.raw` topic
    - [ ] Implement a bash to register schemas in Schema Registry (Optional)
- [ ] Create connectors to install in Kafka Connect (Optional)
    - [ ] Implement a bash to create connectors in Kafka (Optional)
- [ ] Implement a bash to create topics in Kafka (Optional)
- [ ] Save all events inserted in Kafka topics to Minio
