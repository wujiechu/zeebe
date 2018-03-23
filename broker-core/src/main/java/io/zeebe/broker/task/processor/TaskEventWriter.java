package io.zeebe.broker.task.processor;

import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamReader;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.util.sched.ActorControl;

public class TaskEventWriter implements StreamProcessorLifecycleAware
{

    // TODO: mit DeploymentEventWriter vereinheitlichen

    private final TypedStreamWriter writer;
    private final TypedStreamReader reader;

    private ActorControl actor;

    public TaskEventWriter(
            TypedStreamWriter writer,
            TypedStreamReader reader)
    {
        this.writer = writer;
        this.reader = reader;
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        this.actor = streamProcessor.getActor();
    }

    @Override
    public void onClose()
    {
        this.reader.close();
    }

    /**
     * Writes a follow-up event copying all properties of the source event and updating the state.
     */
    public boolean tryWriteTaskEvent(final long sourceEventPosition, TaskState newState)
    {
        final TypedEvent<TaskEvent> event = reader.readValue(sourceEventPosition, TaskEvent.class);

        final TaskEvent taskEvent = event.getValue().setState(newState);

        return writer.writeFollowupEvent(event.getKey(), taskEvent) >= 0;
    }
}
