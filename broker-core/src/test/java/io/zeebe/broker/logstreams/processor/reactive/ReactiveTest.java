package io.zeebe.broker.logstreams.processor.reactive;

import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskHeaders;
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
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactiveTest
{

    private static final Logger LOG = io.zeebe.logstreams.impl.Loggers.LOGSTREAMS_LOGGER;

    /**
     * Is multiplied by 100 ms
     */
    public static final int MAX_WAIT_TIME = 1_200;

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
    private LogStreamWriter logStreamWriter;
    public static final int WORK_COUNT = 10_000_000;
    private List<StreamProcessorController> controllerList = new ArrayList<>();

    @Before
    public void setUp()
    {
        defaultLogStream = LogStreams.createFsLogStream(BufferUtil.wrapString("default"), 0)
                  .actorScheduler(schedulerRule.get())
                  .deleteOnClose(true)
                  .logDirectory(temporaryFolder.getRoot().toString())
                  .build();

        final StreamProcessorContext context = new StreamContextBuilder(0, "reactive", new Processor("no"))
            .actorScheduler(schedulerRule.get())
            .logStream(defaultLogStream)
            .build();

        context.logStreamWriter.wrap(defaultLogStream);
        reactiveStreamProcessorController = new ReactiveStreamProcessorController(context);

        firstController = createStreamProcessController("first");
        controllerList.add(firstController);
        secondController = createStreamProcessController("second");
        controllerList.add(secondController);


        defaultLogStream.openAsync().join();
        defaultLogStream.openLogStreamController().join();
        firstController.openAsync().join();
        secondController.openAsync().join();

        for (int i = 0; i < 3; i++)
        {
            final StreamProcessorController streamProcessController = createStreamProcessController("" + i);
            controllerList.add(streamProcessController);
            streamProcessController.openAsync().join();
        }

        reactiveStreamProcessorController.openAsync().join();


        logStreamWriter = context.logStreamWriter;
        firstWrittenTaskEvent = writeTaskEvent(logStreamWriter);

        TestUtil.waitUntil(() -> defaultLogStream.getCurrentAppenderPosition() > firstWrittenTaskEvent);
    }

    private StreamProcessorController createStreamProcessController(String name)
    {
        final Processor secondProcessor = new Processor(name + "-task");
        final StreamProcessorContext context = new StreamContextBuilder(0, name, secondProcessor)
            .actorScheduler(schedulerRule.get())
            .logStream(defaultLogStream)
            .build();
        return new StreamProcessorController(context);
    }


    @After
    public void close()
    {
        LOG.info("Processed task events {}", taskEventProcessed);
        LOG.info("Processed workflow events {}", workflowEventProcessed);

        reactiveStreamProcessorController.closeAsync().join();
        controllerList.forEach((c) -> c.closeAsync().join());
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

        assertThat(firstTaskEvent.getType()).isNotEqualTo(BufferUtil.wrapString(firstStreamProcessor.taskType));
        assertThat(secondTaskEvent.getType()).isNotEqualTo(BufferUtil.wrapString(secondStreamProcessor.taskType));
    }

    @Test
    public void shouldProcessBunchOfTaskEvents()
    {
        // given
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();

        final long lastEventPos = writeTaskEvents(logStreamWriter, WORK_COUNT);
        TestUtil.waitUntil(() -> defaultLogStream.getCurrentAppenderPosition() > lastEventPos);

        // when
        defaultLogStream.setCommitPosition(lastEventPos);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() >= WORK_COUNT);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);
    }


    @Test
    public void shouldProcessBunchOfTaskEventsWithTwoControllers()
    {
        // given
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, firstController).join();
        reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, secondController).join();

        final long lastEventPos = writeTaskEvents(logStreamWriter, WORK_COUNT);
        TestUtil.waitUntil(() -> defaultLogStream.getCurrentAppenderPosition() > lastEventPos);

        // when
        final long start = System.currentTimeMillis();
        defaultLogStream.setCommitPosition(lastEventPos);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() >= WORK_COUNT * 2, MAX_WAIT_TIME);
        final long end = System.currentTimeMillis();
        LOG.info("Processing takes {} ms", end - start);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);
    }


    @Test
    public void shouldProcessBunchOfTaskEventsWithFiveControllers()
    {
        // given
        for (StreamProcessorController controller : controllerList)
        {
            reactiveStreamProcessorController.registerForEvent(EventType.TASK_EVENT, controller).join();
        }

        final long lastEventPos = writeTaskEvents(logStreamWriter, WORK_COUNT);
        TestUtil.waitUntil(() -> defaultLogStream.getCurrentAppenderPosition() > lastEventPos);
        LOG.info("Wrote {} events to the stream, last position is {}.", WORK_COUNT, lastEventPos);

        // when
        final long start = System.currentTimeMillis();
        defaultLogStream.setCommitPosition(lastEventPos);

        // then
        TestUtil.waitUntil(() -> taskEventProcessed.get() >= WORK_COUNT * controllerList.size(), MAX_WAIT_TIME);
        final long end = System.currentTimeMillis();
        LOG.info("Processing takes {} ms", end - start);
        assertThat(workflowEventProcessed.get()).isEqualTo(0);
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
        LOG.info("Wrote task event on pos {}", position);
        return position;
    }


    private long writeTaskEvents(LogStreamWriter writer, int count)
    {
        BrokerEventMetadata brokerEventMetadata = new BrokerEventMetadata();
        brokerEventMetadata
            .protocolVersion(Protocol.PROTOCOL_VERSION)
            .eventType(EventType.TASK_EVENT);

        TaskEvent taskEvent = new TaskEvent();
        taskEvent
            .setState(TaskState.CREATE)
            .setRetries(3)
            .setPayload(BufferUtil.wrapString("payload"));

        final TaskHeaders headers = taskEvent.headers();
        headers.setBpmnProcessId(BufferUtil.wrapString("processId"))
                .setActivityId(BufferUtil.wrapString("activityId"));

        long lastPosition = 0;
        for (int i = 0; i < count; i++)
        {
            taskEvent.setType(BufferUtil.wrapString("task-type" + i));

            headers
                .setWorkflowDefinitionVersion(i)
                .setWorkflowKey(i + 1)
                .setWorkflowInstanceKey(i + 2)
                .setActivityInstanceKey(i + 3);

            long position = 0;
            while (position <= 0)
            {
                position = writer.metadataWriter(brokerEventMetadata)
                    .valueWriter(taskEvent)
                    .positionAsKey()
                    .tryWrite();
                if (position <= 0)
                {
                    try
                    {
                        Thread.sleep(10);
                    }
                    catch (Exception ex)
                    {
                    }
                }
            }

            assert lastPosition < position;
            lastPosition = position;
        }
        return lastPosition;
    }

    private class Processor implements StreamProcessor
    {
        private final String taskType;
        TaskEventProcessor taskEventProcessor;
        WorkflowEventProcessor workflowEventProcessor;

        public Processor(String taskType)
        {
            this.taskType = taskType;
            this.taskEventProcessor = new TaskEventProcessor(taskType);
            this.workflowEventProcessor = new WorkflowEventProcessor();
        }

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

        public TaskEventProcessor(String taskType)
        {
            taskEvent.setType(BufferUtil.wrapString(taskType));
        }

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
