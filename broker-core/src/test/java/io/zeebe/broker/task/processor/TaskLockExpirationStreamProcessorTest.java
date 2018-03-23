package io.zeebe.broker.task.processor;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.task.TaskQueueManagerService;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;
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

        rule.runStreamProcessor(e -> new TaskExpireLockStreamProcessor(
                e.buildStreamReader(),
                e.buildStreamWriter())
            .createStreamProcessor(e));

        rule.writeEvent(1, taskLocked());
        rule.writeEvent(2, taskLocked());

        // when
        rule.getClock().addTime(TaskQueueManagerService.LOCK_EXPIRATION_INTERVAL.plus(Duration.ofSeconds(1)));

        // then
        final List<TypedEvent<TaskEvent>> expirationEvents = doRepeatedly(
            () -> rule.events()
                .onlyTaskEvents()
                .inState(TaskState.EXPIRE_LOCK)
                .collect(Collectors.toList()))
            .until(l -> l.size() == 2);

        assertThat(expirationEvents).extracting("key").containsExactlyInAnyOrder(1L, 2L);
    }
}
