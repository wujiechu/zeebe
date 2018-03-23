package io.zeebe.broker.logstreams.processor;

import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskHeaders;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.broker.workflow.data.WorkflowEvent;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.processor.StreamProcessorBuilder;
import io.zeebe.logstreams.processor.StreamProcessorController;
import io.zeebe.logstreams.spi.SnapshotStorage;
import io.zeebe.logstreams.spi.SnapshotSupport;
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

public class OldStreamCtrlTest
{

    private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

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

    private List<StreamProcessorController> controllerList = new ArrayList<>();
    private StreamProcessorController firstController;
    private LogStream defaultLogStream;
    private long firstWrittenTaskEvent;
    private StreamProcessorController secondController;
    private LogStreamWriter logStreamWriter;
    public static final int WORK_COUNT = 10_000_000;

    @Before
    public void setUp()
    {
        defaultLogStream = LogStreams.createFsLogStream(BufferUtil.wrapString("default"), 0)
            .actorScheduler(schedulerRule.get())
            .deleteOnClose(true)
            .logDirectory(temporaryFolder.getRoot().toString())
            .build();

        final SnapshotStorage storage = LogStreams.createFsSnapshotStore(temporaryFolder.getRoot().toString()).build();

        logStreamWriter = new LogStreamWriterImpl();
        logStreamWriter.wrap(defaultLogStream);

        firstController = createStreamProcessController(storage, "first");
        controllerList.add(firstController);
        secondController = createStreamProcessController(storage, "second");
        controllerList.add(secondController);

        for (int i = 0; i < 3; i++)
        {
            final StreamProcessorController streamProcessController = createStreamProcessController(storage, "" + i);
            controllerList.add(streamProcessController);
        }

        defaultLogStream.openAsync().join();
        defaultLogStream.openLogStreamController().join();

        firstWrittenTaskEvent = writeTaskEvent(logStreamWriter);

        TestUtil.waitUntil(() -> defaultLogStream.getCurrentAppenderPosition() > firstWrittenTaskEvent);
    }

    private StreamProcessorController createStreamProcessController(SnapshotStorage storage, String name)
    {
        return new StreamProcessorBuilder(0, name, new Processor(name + "-task"))
            .actorScheduler(schedulerRule.get())
            .logStream(defaultLogStream)
            .snapshotStorage(storage)
            .build();
    }

    @After
    public void close()
    {
        LOG.info("Processed task events {}", taskEventProcessed);
        LOG.info("Processed workflow events {}", workflowEventProcessed);

        controllerList.forEach((c) -> c.closeAsync().join());
        defaultLogStream.closeAsync().join();
    }

    @Test
    public void shouldProcessBunchOfTaskEvents()
    {
        // given
        firstController.openAsync().join();
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
        firstController.openAsync().join();
        secondController.openAsync().join();

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
    public void shouldProcessBunchOfTaskEventsWithAllControllers()
    {
        // given
        final long lastEventPos = writeTaskEvents(logStreamWriter, WORK_COUNT);
        TestUtil.waitUntil(() -> defaultLogStream.getCurrentAppenderPosition() > lastEventPos);

        for (StreamProcessorController controller : controllerList)
        {
            controller.openAsync().join();
        }


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

            assert position > lastPosition;
            lastPosition = position;
        }
        return lastPosition;
    }

    private class Processor implements StreamProcessor
    {
        private final String taskType;
        TaskEventProcessor taskEventProcessor;
        WorkflowEventProcessor workflowEventProcessor;
        private BrokerEventMetadata brokerEventMetadata;

        public Processor(String taskType)
        {
            this.taskType = taskType;
            this.taskEventProcessor = new TaskEventProcessor(taskType);
            this.workflowEventProcessor = new WorkflowEventProcessor();
            brokerEventMetadata = new BrokerEventMetadata();
        }

        @Override
        public SnapshotSupport getStateResource()
        {
            return new NoopSnapshotSupport();
        }

        @Override
        public EventProcessor onEvent(LoggedEvent event)
        {
            event.readMetadata(brokerEventMetadata);
            if (brokerEventMetadata.getEventType() == EventType.TASK_EVENT)
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

        public void nextEvent(LoggedEvent ref)
        {
            ref.readValue(taskEvent);
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

        public void nextEvent(LoggedEvent ref)
        {
            ref.readValue(workflowEvent);
        }

        @Override
        public void processEvent()
        {
            // process workflow event
            workflowEventProcessed.getAndIncrement();
        }
    }

}
