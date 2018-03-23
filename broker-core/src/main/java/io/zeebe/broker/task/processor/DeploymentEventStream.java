package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.workflow.data.DeploymentEvent;
import io.zeebe.broker.workflow.data.DeploymentState;

public class DeploymentEventStream extends StreamWrapper<TypedEvent<DeploymentEvent>>
{

    public DeploymentEventStream(Stream<TypedEvent<DeploymentEvent>> wrappedStream)
    {
        super(wrappedStream);
    }

    public DeploymentEventStream inState(DeploymentState state)
    {
        return new DeploymentEventStream(filter(e -> e.getValue().getState() == state));
    }
}
