package demo.flink;

import demo.event.DemoEvent;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class NameTransformerFunctionTest {

    @Test
    public void testMap() {
        NameTransformerFunction transformer = new NameTransformerFunction();
        DemoEvent sourceEvent = DemoEvent.builder().name("test 123 name #!").build();
        DemoEvent expectedEvent = DemoEvent.builder().name("TEST 123 NAME #!").build();
        assertThat(transformer.map(sourceEvent), equalTo(expectedEvent));
    }
}
