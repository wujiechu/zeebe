package io.zeebe.broker.task.processor;

import io.zeebe.broker.Loggers;
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
    public void writeTaskEvent(final long sourceEventPosition, TaskState newState)
    {
        final TypedEvent<TaskEvent> event = reader.readValue(sourceEventPosition, TaskEvent.class);

        final TaskEvent taskEvent = event.getValue().setState(newState);

        // TODO: das hier funktioniert nicht mit mehreren Events, weil #runUntilDone nicht synchron ist,
        //   das event-Objekt aber wiederverwendet wird

        actor.runUntilDone(() ->
        {
            final long position = writer.writeFollowupEvent(event.getKey(), taskEvent);
            if (position >= 0)
            {
                Loggers.SYSTEM_LOGGER.debug("Successfully wrote timeout event to stream");
                actor.done();
            }
            else
            {
                actor.yield();
            }
        });
    }
}
