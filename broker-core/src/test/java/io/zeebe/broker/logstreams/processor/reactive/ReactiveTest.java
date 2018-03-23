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

import static org.assertj.core.api.Assertions.assertThat;

public class ReactiveTest
{

    @Rule
    public ActorSchedulerRule schedulerRule = new ActorSchedulerRule();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final AtomicLong taskEventProcessed = new AtomicLong(0);
    private final AtomicLong workflowEventProcessed = new AtomicLong(0);

    private ReactiveStreamProcessorController reactiveStreamProcessorController;
    private StreamProcessorController firstController;
    private LogStream defaultLogStream;
    private long firstWrittenTaskEvent;
    private StreamProcessorController secondController;

    @Before
    public void setUp()
    {
        defaultLogStream = LogStreams.createFsLogStream(BufferUtil.wrapString("default"), 0)
                  .actorScheduler(schedulerRule.get())
                  .deleteOnClose(true)
                  .logDirectory(temporaryFolder.getRoot().toString())
                  .build();

        final StreamProcessorContext context = new StreamContextBuilder(0, "reactive", new Processor())
            .actorScheduler(schedulerRule.get())
            .logStream(defaultLogStream)
            .build();

        context.logStreamWriter.wrap(defaultLogStream);
        reactiveStreamProcessorController = new ReactiveStreamProcessorController(context);

        final Processor firstProcessor = new Processor();
        final StreamProcessorContext firstControllerCtx = new StreamContextBuilder(0, "first", firstProcessor)
            .actorScheduler(schedulerRule.get())
            .logStream(defaultLogStream)
            .build();
        firstController = new StreamProcessorController(firstControllerCtx);


        final Processor secondProcessor = new Processor();
        final StreamProcessorContext secondControllerCtx = new StreamContextBuilder(0, "second", secondProcessor)
            .actorScheduler(schedulerRule.get())
            .logStream(defaultLogStream)
            .build();
        secondController = new StreamProcessorController(secondControllerCtx);

        defaultLogStream.openAsync().join();
        defaultLogStream.openLogStreamController().join();
        firstController.openAsync().join();
        secondController.openAsync().join();
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
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();

        // when
        defaultLogStream.setCommitPosition(firstWrittenTaskEvent);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() == 1);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);
    }

    @Test
    public void shouldDistributeOnlyTaskEventsControllerForOtherEventsShouldNotBeenCalled()
    {
        // given
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();
        reactiveStreamProcessorController.registerForEvent(EventType.WORKFLOW_EVENT, secondController).join();

        // when
        defaultLogStream.setCommitPosition(firstWrittenTaskEvent);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() == 1);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);
    }

    @Test
    public void shouldDistributeTaskEventsOnTwiceRegistration()
    {
        // given
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();

        // when
        defaultLogStream.setCommitPosition(firstWrittenTaskEvent);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() == 2);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);
    }

    @Test
    public void shouldDistributeTaskEventsOnTwoSeparateControllers()
    {
        // given
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, secondController).join();

        // when
        defaultLogStream.setCommitPosition(firstWrittenTaskEvent);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() == 2);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);
    }

    @Test
    public void shouldCopyEventRefForEachProcessing()
    {
        // given
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, secondController).join();
        defaultLogStream.setCommitPosition(firstWrittenTaskEvent);

        // when
        TestUtil.waitUntil(() -> taskEventProcessed.get() == 2);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);

        // then
        final Processor firstStreamProcessor = (Processor) firstController.getStreamProcessor();
        final TaskEventProcessor firstTaskEventProcessor = firstStreamProcessor.taskEventProcessor;
        final TaskEvent firstTaskEvent = firstTaskEventProcessor.taskEvent;

        final Processor secondStreamProcessor = (Processor) secondController.getStreamProcessor();
        final TaskEventProcessor secondTaskEventProcessor = secondStreamProcessor.taskEventProcessor;
        final TaskEvent secondTaskEvent = secondTaskEventProcessor.taskEvent;

        assertThat(firstTaskEvent != secondTaskEvent).isTrue();
        assertThat(firstTaskEvent).isEqualTo(secondTaskEvent);
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
        TaskEventProcessor taskEventProcessor = new TaskEventProcessor();
        WorkflowEventProcessor workflowEventProcessor = new WorkflowEventProcessor();

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
