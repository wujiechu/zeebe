package io.zeebe.broker.task.processor;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import io.zeebe.broker.task.TaskQueueManagerService;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.broker.topic.Events;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.util.buffer.BufferUtil;

public class TaskLockExpirationStreamProcessorTest
{

    @Rule
    public StreamProcessorRule rule = new StreamProcessorRule();

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
        rule.getClock().pinCurrentTime();

        final StreamProcessorControl control = rule.runStreamProcessor(e -> new TaskExpireLockStreamProcessor(
                e.buildStreamReader(),
                e.buildStreamWriter())
            .createStreamProcessor(e));

        // TODO: das hier ist nicht intuitiv
        control.blockAfterEvent(e -> false);
        control.unblock();

        rule.writeEvent(1, taskLocked());
        rule.writeEvent(2, taskLocked());

        // when
        rule.getClock().addTime(TaskQueueManagerService.LOCK_EXPIRATION_INTERVAL.plus(Duration.ofSeconds(1)));

        // then

        // TODO: vll kann man hier auch eigene Subklassen haben, die nettere Filtermethoden haben, z.B. filterByState(..)
        final List<Long> expirationEvents = doRepeatedly(
            () -> rule.events()
                .filter(Events::isTaskEvent)
                .filter(e -> Events.asTaskEvent(e).getState() == TaskState.EXPIRE_LOCK)
                .map(e -> e.getKey())
                .collect(Collectors.toList()))
            .until(l -> l.size() == 2);

        assertThat(expirationEvents).containsExactlyInAnyOrder(1L, 2L);
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
