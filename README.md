# Flink Integration with Kafka Demo

## Overview

Demonstrates running a Flink job that listens to Kafka for messages, transforms the message, then emits the transformed message back to Kafka.

<div style="text-align: center;">
    <img src="resources/flink-kafka-connector.png" alt="Flink demo with Kafka integration" style="max-width: 800px; display: block; margin: 0 auto;" />
</div>

## Running the Demo

The demo spins up Flink and Kafka in Docker containers, and steps through building the Flink application and submitting the job via the Flink console.  The Kafka command line tools are used to produce a message to Kafka which the Flink job processes by uppercasing the name, and to consume the resulting transformed message.  The message is in JSON format, containing a name field.  The Flink Kafka Source and Kafka Sink use a JSON deserializer and serializer respectively to marshal the message. 

Build the jar containing the Flink job:
```
mvn clean install
```

Bring up the Flink job and task managers, along with Kafka (and Zookeeper), in Docker containers (defined in `docker-compose.yml`):
```
docker-compose up -d
```

Ensure topics are created before the Flink job is submitted:
```
docker exec -ti kafka kafka-topics --create --topic=demo-inbound --partitions 3 --if-not-exists --bootstrap-server=kafka:9093
docker exec -ti kafka kafka-topics --create --topic=demo-outbound --partitions 3 --if-not-exists --bootstrap-server=kafka:9093
docker exec -ti kafka kafka-topics --list --bootstrap-server=kafka:9093
```

Navigate to the Flink console:
```
localhost:8081
```

Submit the new job:
`Add New` `flink-demo-1.0.0.jar` from the `target` dir.  Click on the jar and enter `Program Arguments`: `kafka:9093`.  Click `Submit`.

<div style="text-align: center;">
    <img src="resources/flink-submit-job.png" alt="Flink submit new job" style="border: 1px solid black; max-width: 800px; display: block; margin: 0 auto;" />
</div>

The job will now be shown in the `Running Jobs` section:

<div style="text-align: center;">
    <img src="resources/flink-running-job.png" alt="Flink job running" style="border: 1px solid black; max-width: 800px; display: block; margin: 0 auto;" />
</div>

Alternatively to run the Flink application manually (or manually with remote debug enabled):
```
java -jar target/flink-demo-1.0.0.jar
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -jar target/flink-kafka-connector-1.0.0.jar
```

Produce a message to Kafka, which the Flink job will consume:
```
docker exec -ti kafka kafka-console-producer --topic demo-inbound --bootstrap-server kafka:9093
{"name":"abcdefg"}
```

Consume the message from Kafka, which the Flink job will produce
```
docker exec -ti kafka kafka-console-consumer --topic demo-outbound --bootstrap-server kafka:9093 --from-beginning
```
Expected output with uppercased name:  `{"name":"ABCDEFG"}`
