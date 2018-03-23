package io.zeebe.broker.task.processor;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.task.TaskQueueManagerService;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.broker.topic.Events;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.broker.topic.TestStreams;
import io.zeebe.broker.transport.clientapi.BufferingServerOutput;
import io.zeebe.broker.workflow.data.DeploymentState;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.clock.ControlledActorClock;
import io.zeebe.util.sched.testing.ActorSchedulerRule;

public class TaskLockExpirationStreamProcessorTest
{

    public static final String STREAM_NAME = "stream";

    public TemporaryFolder tempFolder = new TemporaryFolder();
    public AutoCloseableRule closeables = new AutoCloseableRule();

    public ControlledActorClock clock = new ControlledActorClock();
    public ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule(clock);

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(tempFolder).around(actorSchedulerRule).around(closeables);

    public BufferingServerOutput output;

    protected TestStreams streams;

    private StreamProcessor streamProcessor;

    // TODO: vielleicht kann man das hier mal in eine Rule abstrahieren (und auch die ganzen anderen Rules darin kapseln)
    @Before
    public void setUp()
    {
        output = new BufferingServerOutput();

        streams = new TestStreams(tempFolder.getRoot(), closeables, actorSchedulerRule.get());
        streams.createLogStream(STREAM_NAME);

        streams.newEvent(STREAM_NAME) // TODO: workaround for https://github.com/zeebe-io/zeebe/issues/478
            .event(new UnpackedObject())
            .write();

        rebuildStreamProcessor();
    }

    private void rebuildStreamProcessor()
    {
        final TypedStreamEnvironment streamEnvironment = new TypedStreamEnvironment(streams.getLogStream(STREAM_NAME), output);

        final TaskExpireLockStreamProcessor taskProcessor =
                new TaskExpireLockStreamProcessor(streamEnvironment.buildStreamReader(), streamEnvironment.buildStreamWriter());

        streamProcessor = taskProcessor.createStreamProcessor(streamEnvironment);
    }

    private long writeEventToStream(long key, UnpackedObject object)
    {
        return streams.newEvent(STREAM_NAME)
            .event(object)
            .key(key)
            .write();
    }

    private TaskEvent taskLocked()
    {
        final TaskEvent event = new TaskEvent();

        event.setState(TaskState.LOCKED);
        event.setType(BufferUtil.wrapString("foo"));

        return event;
    }

    @Test
    public void shouldExpireLockIfAfterLockTimeForTwoTasks()
    {
        // given
        clock.pinCurrentTime();

        // TODO: die folgenden drei Instruktionen sind copy-paste
        final StreamProcessorControl control = streams.runStreamProcessor(STREAM_NAME, streamProcessor);

        // TODO: das hier ist nicht intuitiv
        control.blockAfterEvent(e -> false);
        control.unblock();

        writeEventToStream(1, taskLocked());
        writeEventToStream(2, taskLocked());

        // when
        clock.addTime(TaskQueueManagerService.LOCK_EXPIRATION_INTERVAL.plus(Duration.ofSeconds(1)));

        // then

        // TODO: vll kann man hier auch eigene Subklassen haben, die nettere Filtermethoden haben, z.B. filterByState(..)
        final List<LoggedEvent> expirationEvents = doRepeatedly(
            () -> streams.events(STREAM_NAME)
                .filter(Events::isTaskEvent)
                .filter(e -> Events.asTaskEvent(e).getState() == TaskState.EXPIRE_LOCK)
                .collect(Collectors.toList()))
            .until(l -> l.size() == 2);

        assertThat(expirationEvents).extracting("key").containsExactlyInAnyOrder(1, 2);
    }

    @Test
    public void shouldExpireLockOnlyOnce()
    {
        // TODO: expire task twice, then make sure the second expire command is rejected
        // TODO: das hier gehört eher in einen TaskStreamProcessorTest
        // und die anderen auch
        fail();
    }

    @Test
    public void shouldRejectExpireCommandIfTaskCompleted()
    {
        fail();
    }

    @Test
    public void shouldRejectExpireCommandIfTaskFailed()
    {
        fail();
    }

    @Test
    public void shouldFailToExpireLockIfEventNotFound()
    {
        fail();
    }
}
