package io.zeebe.broker.task.processor;

import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.system.log.PendingPartitionsIndex;
import io.zeebe.broker.system.log.ResolvePendingPartitionsCommand;
import io.zeebe.broker.system.log.SystemPartitionManager;
import io.zeebe.broker.system.log.TopicsIndex;
import io.zeebe.broker.topic.TestPartitionManager;
import io.zeebe.broker.topic.TestStreams;
import io.zeebe.broker.transport.clientapi.BufferingServerOutput;
import io.zeebe.test.util.AutoCloseableRule;
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

    private void rebuildStreamProcessor()
    {
        final TopicsIndex topicsIndex = new TopicsIndex();
        final PendingPartitionsIndex partitionsIndex = new PendingPartitionsIndex();

        final TypedStreamEnvironment streamEnvironment = new TypedStreamEnvironment(streams.getLogStream(STREAM_NAME), output);

        TaskExpireLockStreamProcessor streamProcessor = new TaskExpireLockStreamProcessor();

        checkPartitionsCmd = new ResolvePendingPartitionsCommand(
                partitionsIndex,
                partitionManager,
                streamEnvironment.buildStreamReader(),
                streamEnvironment.buildStreamWriter());

        streamProcessor = SystemPartitionManager
                .buildTopicCreationProcessor(
                    streamEnvironment,
                    partitionManager,
                    topicsIndex,
                    partitionsIndex,
                    CREATION_EXPIRATION,
                    () ->
                    { });
    }

    @Test
    public void shouldExpireLockIfAfterLockTimeForTwoTasks()
    {
        // TODO: should expire two tasks in one invocation of the check
        fail();
    }

    @Test
    public void shouldExpireLockOnlyOnce()
    {
        // TODO: expire task twice, then make sure the second expire command is rejected
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
