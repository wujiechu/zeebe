package io.zeebe.broker.task.processor;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.task.TaskSubscriptionManager;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.util.buffer.BufferUtil;

public class TaskInstanceStreamProcessorTest
{

    @Rule
    public StreamProcessorRule rule = new StreamProcessorRule();

    @Mock
    public TaskSubscriptionManager subscriptionManager;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void todo()
    {
        fail("port tests from old test");
    }

    @Test
    public void shouldCompleteExpiredTask()
    {
        fail("implement");
    }

    @Test
    public void shouldLockOnlyOnce()
    {
        fail("implement");
    }

    @Test
    public void shouldRejectLockIfTaskNotFound()
    {
        fail("implement");
    }

    @Test
    public void shouldRejectLockIfTaskAlreadyLocked()
    {

        fail("implement");
    }

    @Test
    public void shouldExpireLockOnlyOnce()
    {
        // given
        final long key = 1;
        final StreamProcessorControl control = rule.runStreamProcessor(this::buildStreamProcessor);

        rule.writeEvent(key, create());
        waitForEventInState(TaskState.CREATED);

        control.blockAfterTaskEvent(e -> e.getValue().getState() == TaskState.LOCKED);
        rule.writeEvent(key, lock(rule.getClock().getCurrentTime().plusSeconds(30)));
        waitForEventInState(TaskState.LOCKED);

        // when
        rule.writeEvent(key, expireLock());
        rule.writeEvent(key, expireLock());
        control.unblock();

        // then
        waitForEventInState(TaskState.LOCK_EXPIRATION_REJECTED);

        final List<TypedEvent<TaskEvent>> taskEvents = rule.events().onlyTaskEvents().collect(Collectors.toList());
        assertThat(taskEvents).extracting("value.state")
            .containsExactly(
                    TaskState.CREATE,
                    TaskState.CREATED,
                    TaskState.LOCK,
                    TaskState.LOCKED,
                    TaskState.EXPIRE_LOCK,
                    TaskState.EXPIRE_LOCK,
                    TaskState.LOCK_EXPIRED,
                    TaskState.LOCK_EXPIRATION_REJECTED);
    }


    @Test
    public void shouldRejectExpireCommandIfTaskCREATED()
    {
        fail("adapt");

        // given
        final long key = 1;
        rule.runStreamProcessor(this::buildStreamProcessor);

        // when
        rule.writeEvent(key, expireLock());

        // then
        waitForEventInState(TaskState.LOCK_EXPIRATION_REJECTED);

        final List<TypedEvent<TaskEvent>> taskEvents = rule.events().onlyTaskEvents().collect(Collectors.toList());
        assertThat(taskEvents).extracting("value.state")
            .containsExactly(
                    TaskState.EXPIRE_LOCK,
                    TaskState.LOCK_EXPIRATION_REJECTED);
    }

    @Test
    public void shouldRejectExpireCommandIfTaskCompleted()
    {
        // given
        final long key = 1;
        final StreamProcessorControl control = rule.runStreamProcessor(this::buildStreamProcessor);

        rule.writeEvent(key, create());
        waitForEventInState(TaskState.CREATED);

        control.blockAfterTaskEvent(e -> e.getValue().getState() == TaskState.LOCKED);
        rule.writeEvent(key, lock(rule.getClock().getCurrentTime().plusSeconds(30)));
        waitForEventInState(TaskState.LOCKED);

        // when
        rule.writeEvent(key, complete());
        rule.writeEvent(key, expireLock());
        control.unblock();

        // then
        waitForEventInState(TaskState.LOCK_EXPIRATION_REJECTED);

        final List<TypedEvent<TaskEvent>> taskEvents = rule.events().onlyTaskEvents().collect(Collectors.toList());
        assertThat(taskEvents).extracting("value.state")
            .containsExactly(
                    TaskState.CREATE,
                    TaskState.CREATED,
                    TaskState.LOCK,
                    TaskState.LOCKED,
                    TaskState.COMPLETE,
                    TaskState.EXPIRE_LOCK,
                    TaskState.COMPLETED,
                    TaskState.LOCK_EXPIRATION_REJECTED);
    }


    @Test
    public void shouldRejectExpireCommandIfTaskFailed()
    {
        // given
        final long key = 1;
        final StreamProcessorControl control = rule.runStreamProcessor(this::buildStreamProcessor);

        rule.writeEvent(key, create());
        waitForEventInState(TaskState.CREATED);

        control.blockAfterTaskEvent(e -> e.getValue().getState() == TaskState.LOCKED);
        rule.writeEvent(key, lock(rule.getClock().getCurrentTime().plusSeconds(30)));
        waitForEventInState(TaskState.LOCKED);

        // when
        rule.writeEvent(key, failure());
        rule.writeEvent(key, expireLock());
        control.unblock();

        // then
        waitForEventInState(TaskState.LOCK_EXPIRATION_REJECTED);

        final List<TypedEvent<TaskEvent>> taskEvents = rule.events().onlyTaskEvents().collect(Collectors.toList());
        assertThat(taskEvents).extracting("value.state")
            .containsExactly(
                    TaskState.CREATE,
                    TaskState.CREATED,
                    TaskState.LOCK,
                    TaskState.LOCKED,
                    TaskState.FAIL,
                    TaskState.EXPIRE_LOCK,
                    TaskState.FAILED,
                    TaskState.LOCK_EXPIRATION_REJECTED);
    }

    @Test
    public void shouldRejectExpireCommandIfTaskNotFound()
    {
        // given
        final long key = 1;
        rule.runStreamProcessor(this::buildStreamProcessor);

        // when
        rule.writeEvent(key, expireLock());

        // then
        waitForEventInState(TaskState.LOCK_EXPIRATION_REJECTED);

        final List<TypedEvent<TaskEvent>> taskEvents = rule.events().onlyTaskEvents().collect(Collectors.toList());
        assertThat(taskEvents).extracting("value.state")
            .containsExactly(
                    TaskState.EXPIRE_LOCK,
                    TaskState.LOCK_EXPIRATION_REJECTED);
    }

    private void waitForEventInState(TaskState state)
    {
        waitUntil(() -> rule.events().onlyTaskEvents().inState(state).findFirst().isPresent());
    }

    private TaskEvent create()
    {
        final TaskEvent event = new TaskEvent();

        event.setState(TaskState.CREATE);
        event.setType(BufferUtil.wrapString("foo"));

        return event;
    }

    private TaskEvent lock(Instant lockTime)
    {
        final TaskEvent event = new TaskEvent();

        event.setState(TaskState.LOCK);
        event.setType(BufferUtil.wrapString("foo"));
        event.setLockOwner(BufferUtil.wrapString("bar"));
        event.setLockTime(lockTime.toEpochMilli());

        return event;
    }

    private TaskEvent complete()
    {
        final TaskEvent event = new TaskEvent();

        event.setState(TaskState.COMPLETE);
        event.setType(BufferUtil.wrapString("foo"));
        event.setLockOwner(BufferUtil.wrapString("bar"));

        return event;
    }

    private TaskEvent failure()
    {
        final TaskEvent event = new TaskEvent();

        event.setState(TaskState.FAIL);
        event.setType(BufferUtil.wrapString("foo"));
        event.setLockOwner(BufferUtil.wrapString("bar"));

        return event;
    }

    private TaskEvent expireLock()
    {
        final TaskEvent event = new TaskEvent();

        event.setState(TaskState.EXPIRE_LOCK);
        event.setType(BufferUtil.wrapString("foo"));
        event.setLockOwner(BufferUtil.wrapString("bar"));

        return event;
    }

    private StreamProcessor buildStreamProcessor(TypedStreamEnvironment env)
    {
        return new TaskInstanceStreamProcessor(subscriptionManager).createStreamProcessor(env);
    }


}
