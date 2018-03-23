package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.system.log.TopicState;

public class TopicEventStream extends StreamWrapper<TypedEvent<TopicEvent>>
{

    public TopicEventStream(Stream<TypedEvent<TopicEvent>> wrappedStream)
    {
        super(wrappedStream);
    }

    public TopicEventStream inState(TopicState state)
    {
        return new TopicEventStream(filter(e -> e.getValue().getState() == state));
    }
}
