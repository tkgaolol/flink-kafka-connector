package demo.flink;

import demo.event.DemoEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

@Slf4j
public class NameTransformerFunction implements MapFunction<DemoEvent, DemoEvent> {

    @Override
    public DemoEvent map(DemoEvent event) {
        log.info("Converting event name from {} to {}", event.getName(), event.getName().toUpperCase());
        event.setName(event.getName().toUpperCase());
        return event;
    }
}
