package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.system.log.PartitionEvent;
import io.zeebe.broker.system.log.PartitionState;

public class PartitionEventStream extends StreamWrapper<TypedEvent<PartitionEvent>>
{

    public PartitionEventStream(Stream<TypedEvent<PartitionEvent>> wrappedStream)
    {
        super(wrappedStream);
    }

    public PartitionEventStream inState(PartitionState state)
    {
        return new PartitionEventStream(filter(e -> e.getValue().getState() == state));
    }
}
