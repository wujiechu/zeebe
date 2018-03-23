package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;

public class TaskEventStream extends StreamWrapper<TypedEvent<TaskEvent>>
{

    public TaskEventStream(Stream<TypedEvent<TaskEvent>> wrappedStream)
    {
        super(wrappedStream);
    }

    public TaskEventStream inState(TaskState state)
    {
        return new TaskEventStream(filter(e -> e.getValue().getState() == state));
    }
}
