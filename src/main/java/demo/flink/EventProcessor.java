package demo.flink;

import java.util.Properties;

import demo.event.DemoEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@Slf4j
public class EventProcessor {

    private static final String TOPIC_INBOUND = "demo-inbound";
    private static final String TOPIC_OUTBOUND = "demo-outbound";
    private static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";

    public static void main(String[] args) {
        String bootstrapServers = BOOTSTRAP_SERVERS_DEFAULT;
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        log.info("Bootstrap servers URL: {}", bootstrapServers);

        try {
            final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<DemoEvent> eventInputStream = environment
                .fromSource(kafkaSource(bootstrapServers), WatermarkStrategy.noWatermarks(), "Kafka Source");

            eventInputStream.map(event -> {
                    log.info("Converting event name from {} to {}", event.getName(), event.getName().toUpperCase());
                    event.setName(event.getName().toUpperCase());
                    return event;
                })
                .sinkTo(kafkaSink(bootstrapServers));

            environment.execute("EventProcessor Job");
            log.info("Started stream execution environment.");
        } catch (Exception e) {
            log.error("Failed to execute Flink job", e);
            throw new RuntimeException("Failed to execute Flink job", e);
        }
    }

    private static KafkaSource<DemoEvent> kafkaSource(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);

        return KafkaSource.<DemoEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setProperties(properties)
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(DemoEvent.class))
                .setTopics(TOPIC_INBOUND)
                .build();
    }

    private static KafkaSink<DemoEvent> kafkaSink(String bootstrapServers) {
        return KafkaSink.<DemoEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<DemoEvent>builder()
                                .setValueSerializationSchema(new JsonSerializationSchema<>())
                                .setTopic(TOPIC_OUTBOUND)
                                .build())
                .build();
    }
}
