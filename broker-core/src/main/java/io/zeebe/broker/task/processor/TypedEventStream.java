package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.system.log.PartitionEvent;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.topic.Events;
import io.zeebe.broker.workflow.data.DeploymentEvent;
import io.zeebe.logstreams.log.LoggedEvent;

public class TypedEventStream extends StreamWrapper<LoggedEvent>
{

    public TypedEventStream(Stream<LoggedEvent> stream)
    {
        super(stream);
    }

    public TaskEventStream onlyTaskEvents()
    {
        return new TaskEventStream(
            filter(Events::isTaskEvent)
            .map(e -> CopiedTypedEvent.toTypedEvent(e, TaskEvent.class)));
    }


    public DeploymentEventStream onlyDeploymentEvents()
    {
        return new DeploymentEventStream(
            filter(Events::isDeploymentEvent)
            .map(e -> CopiedTypedEvent.toTypedEvent(e, DeploymentEvent.class)));
    }

    public PartitionEventStream onlyPartitionEvents()
    {
        return new PartitionEventStream(
            filter(Events::isPartitionEvent)
            .map(e -> CopiedTypedEvent.toTypedEvent(e, PartitionEvent.class)));
    }


    public TopicEventStream onlyTopicEvents()
    {
        return new TopicEventStream(
            filter(Events::isTopicEvent)
            .map(e -> CopiedTypedEvent.toTypedEvent(e, TopicEvent.class)));
    }


}
