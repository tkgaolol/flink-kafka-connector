package demo.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import demo.event.DemoEvent;
import demo.flink.NameTransformerFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

public class EventProcessorIntegrationTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testNameTransformPipeline() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        CollectSink.values.clear();
        environment.fromElements(DemoEvent.builder().name("John Smith").build(),
                        DemoEvent.builder().name("Steve Jones").build(),
                        DemoEvent.builder().name("Alan Brown").build())
                .map(new NameTransformerFunction())
                .addSink(new CollectSink());
        environment.execute();
        assertThat(CollectSink.values, hasItems(DemoEvent.builder().name("JOHN SMITH").build(),
                DemoEvent.builder().name("STEVE JONES").build(),
                DemoEvent.builder().name("ALAN BROWN").build()));
    }

    private static class CollectSink implements SinkFunction<DemoEvent> {

        public static final List<DemoEvent> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(DemoEvent value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }
}
