package demo.flink;

import demo.event.DemoEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class EventProcessor {

    private static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";
    private static final String SOURCE_TOPIC = "demo-inbound";
    private static final String SINK_TOPIC = "demo-outbound";

    public static void main(String[] args) {
        String bootstrapServers = BOOTSTRAP_SERVERS_DEFAULT;
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        log.info("Bootstrap servers URL: {}", bootstrapServers);

        log.info("Flink job starting");  // Logging used for component test to check container started.
        KafkaConnectorFactory factory = new KafkaConnectorFactory(bootstrapServers);
        execute(StreamExecutionEnvironment.getExecutionEnvironment(),
                factory.kafkaSource(SOURCE_TOPIC),
                factory.kafkaSink(SINK_TOPIC));
        log.info("Flink job has finished.");
    }

    protected static void execute(StreamExecutionEnvironment environment, KafkaSource<DemoEvent> kafkaSource, KafkaSink<DemoEvent> kafkaSink) {
        try {
            DataStream<DemoEvent> eventInputStream = environment
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
            eventInputStream.map(new NameTransformerFunction())
                .sinkTo(kafkaSink);
            environment.execute("EventProcessor Job");
        } catch (Exception e) {
            log.error("Failed to execute Flink job", e);
            throw new RuntimeException("Failed to execute Flink job", e);
        }
    }
}
