package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.incident.data.IncidentEvent;
import io.zeebe.broker.incident.data.IncidentState;
import io.zeebe.broker.logstreams.processor.TypedEvent;

public class IncidentEventStream extends StreamWrapper<TypedEvent<IncidentEvent>>
{

    public IncidentEventStream(Stream<TypedEvent<IncidentEvent>> wrappedStream)
    {
        super(wrappedStream);
    }

    public IncidentEventStream inState(IncidentState state)
    {
        return new IncidentEventStream(filter(e -> e.getValue().getState() == state));
    }
}
