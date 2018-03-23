package io.zeebe.broker.logstreams.processor.reactive;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.broker.workflow.data.WorkflowEvent;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.test.util.TestUtil;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.atomic.AtomicLong;

public class ReactiveTest
{

    @Rule
    public ActorSchedulerRule schedulerRule = new ActorSchedulerRule();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final AtomicLong taskEventProcessed = new AtomicLong(0);
    private final AtomicLong workflowEventProcessed = new AtomicLong(0);

    private ReactiveStreamProcessorController reactiveStreamProcessorController;
    private StreamProcessorController controller;
    private LogStream defaultLogStream;
    private long firstWrittenTaskEvent;

    @Before
    public void setUp()
    {
        final Processor processor = new Processor();
        defaultLogStream = LogStreams.createFsLogStream(BufferUtil.wrapString("default"), 0)
                  .actorScheduler(schedulerRule.get())
                  .deleteOnClose(true)
                  .logDirectory(temporaryFolder.getRoot().toString())
                  .build();

        final StreamProcessorContext context = new StreamContextBuilder(0, "reactive", processor)
            .actorScheduler(schedulerRule.get())
            .logStream(defaultLogStream)
            .build();

        reactiveStreamProcessorController = new ReactiveStreamProcessorController(context);

        controller = new StreamProcessorController(context);

        defaultLogStream.openAsync().join();
        defaultLogStream.openLogStreamController().join();
        controller.openAsync().join();
        reactiveStreamProcessorController.openAsync().join();

        firstWrittenTaskEvent = writeTaskEvent(context.logStreamWriter);

    }

    @After
    public void close()
    {
        defaultLogStream.closeAsync().join();

    }

    @Test
    public void shouldDistributeOnlyTaskEvents()
    {
        // given
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, controller).join();

        // when
        defaultLogStream.setCommitPosition(firstWrittenTaskEvent);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() == 1);
    }


    private long writeTaskEvent(LogStreamWriter writer)
    {
        BrokerEventMetadata brokerEventMetadata = new BrokerEventMetadata();
        brokerEventMetadata
            .protocolVersion(Protocol.PROTOCOL_VERSION)
            .eventType(EventType.TASK_EVENT);

        TaskEvent taskEvent = new TaskEvent();

        taskEvent
            .setState(TaskState.CREATE)
            .setType(BufferUtil.wrapString("task-type"))
            .setRetries(3)
            .setPayload(BufferUtil.wrapString("payload"));

        taskEvent.headers()
            .setBpmnProcessId(BufferUtil.wrapString("processId"))
            .setWorkflowDefinitionVersion(1)
            .setWorkflowKey(123)
            .setWorkflowInstanceKey(124)
            .setActivityId(BufferUtil.wrapString("activityId"))
            .setActivityInstanceKey(125);

        final long position = writer.metadataWriter(brokerEventMetadata)
            .valueWriter(taskEvent)
            .positionAsKey()
            .tryWrite();

        assert position > 0;
        Loggers.SYSTEM_LOGGER.debug("Wrote task event on pos {}", position);
        return position;
    }

    private class Processor implements StreamProcessor
    {
        private TaskEventProcessor taskEventProcessor = new TaskEventProcessor();
        private WorkflowEventProcessor workflowEventProcessor = new WorkflowEventProcessor();

        @Override
        public EventProcessor onEvent(EventRef event)
        {
            if (event.getType() == EventType.TASK_EVENT)
            {
                taskEventProcessor.nextEvent(event);
                return taskEventProcessor;
            }
            else
            {
                workflowEventProcessor.nextEvent(event);
                return workflowEventProcessor;
            }
        }
    }

    private class TaskEventProcessor implements EventProcessor
    {
        TaskEvent taskEvent = new TaskEvent();

        public void nextEvent(EventRef ref)
        {
            ref.copyEvent(taskEvent);
        }

        @Override
        public void processEvent()
        {
            taskEventProcessed.getAndIncrement();
        }
    }

    private class WorkflowEventProcessor implements EventProcessor
    {
        WorkflowEvent workflowEvent = new WorkflowEvent();

        public void nextEvent(EventRef ref)
        {
            ref.copyEvent(workflowEvent);
        }

        @Override
        public void processEvent()
        {
            // process workflow event
            workflowEventProcessed.getAndIncrement();
        }
    }
}
