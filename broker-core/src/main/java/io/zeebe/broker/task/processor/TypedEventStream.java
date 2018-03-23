package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.topic.Events;
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


}
