package io.zeebe.broker.task.processor;

import static org.agrona.BitUtil.SIZE_OF_LONG;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.agrona.DeadlineTimerWheel;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedEventProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamReader;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.map.Long2BytesZbMap;
import io.zeebe.map.iterator.Long2BytesZbMapEntry;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.util.sched.ScheduledTimer;
import io.zeebe.util.sched.clock.ActorClock;

public class TaskExpireLockStreamProcessor2 implements StreamProcessorLifecycleAware
{
    private static final int MAP_VALUE_MAX_LENGTH = SIZE_OF_LONG + SIZE_OF_LONG;
    private Long2BytesZbMap expirationMap = new Long2BytesZbMap(MAP_VALUE_MAX_LENGTH);

    private UnsafeBuffer mapAccessBuffer = new UnsafeBuffer(new byte[MAP_VALUE_MAX_LENGTH]);

    private DeadlineTimerWheel timerWheel;
    private Long2LongHashMap timerTaskMapping;
    private Long2LongHashMap taskTimerMapping;

    private final TaskEventWriter streamWriter;

    public TaskExpireLockStreamProcessor2(TypedStreamReader streamReader, TypedStreamWriter streamWriter)
    {
        this.streamWriter = new TaskEventWriter(streamWriter, streamReader);
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        // TODO: params

        Duration timerGranularity = Duration.ofMillis(1024);


        long now = ActorClock.currentTimeMillis();

        timerWheel = new DeadlineTimerWheel(
                TimeUnit.MILLISECONDS,
                now,
                (int) timerGranularity.toMillis(),
                1024);
        timerTaskMapping = new Long2LongHashMap(-1);
        taskTimerMapping = new Long2LongHashMap(-1);

        for (Long2BytesZbMapEntry mapEntry : expirationMap)
        {
            final DirectBuffer value = mapEntry.getValue();
            addTaskToWheel(mapEntry.getKey(), value.getLong(SIZE_OF_LONG));
        }

        // TODO: resolution couild be configurable
        ScheduledTimer timer = streamProcessor
            .getActor()
            .runAtFixedRate(timerGranularity, this::timeOutTasks);

        Loggers.SYSTEM_LOGGER.debug("Timer wheel start time: {}", now);
        Loggers.SYSTEM_LOGGER.debug("Scheduling task expiration timer {}", System.identityHashCode(timer));
    }

    @Override
    public void onClose()
    {
        timerWheel = null;
        timerTaskMapping = null;
        taskTimerMapping = null;
    }

    private void timeOutTasks()
    {
        final long now = ActorClock.currentTimeMillis();
        Loggers.SYSTEM_LOGGER.debug("Running timeout check at {}", now);

        // TODO: limit the max number?

        if (timerWheel.timerCount() > 0)
        {
            // TODO:
            // * make sure we spin the entire wheel only once at a time (because all due timers are
            //   found in that cycle)
            // * cannot tell from the outside if the wheel stopped in the current tick because
            //   of handler backpressure (and due timers remain) or because the next tick
            //   is not due (and no due timers remain)
            // * it can happen that the tick falls very very far behind real time. Then #currentTickTime
            //   returns rather old times and we would spin the wheel a lot although we can already know that
            //   there are no due timers wrt. to the last time we spun the entire wheel
            do
            {
                // need to poll more than once because one invocation only advances one tick
                timerWheel.poll(now, this::onTimeout, Integer.MAX_VALUE);
            }
            while (timerWheel.currentTickTime() <= now);
        }

        int timedOutDeployments = timerWheel.poll(now, this::onTimeout, Integer.MAX_VALUE);


        Loggers.SYSTEM_LOGGER.debug("Timed out deployments: {}", timedOutDeployments);
    }

    private boolean onTimeout(TimeUnit timeUnit, long now, long timerId)
    {
        final long taskKey = timerTaskMapping.get(timerId);

        System.out.println("Timing out task with key " + taskKey);

        if (taskKey >= 0)
        {
            final long sourceEventPosition = expirationMap.get(taskKey).getLong(0);

            // TODO: das hier kann eine große Menge an submitteten ActorJobs geben, wenn viele Timer gleichzeitig expired sind;
            // sollte lieber mit backpressure arbeiten
            // und ohne actor.runUntilDone
            streamWriter.writeDeploymentEvent(sourceEventPosition, TaskState.EXPIRE_LOCK);
        }

        return true;

    }

    private void addTaskToWheel(long taskKey, long deadline)
    {
        Loggers.SYSTEM_LOGGER.debug("Adding task {} with deadline {} to timer wheel", taskKey, deadline);

        final long timerId = timerWheel.scheduleTimer(deadline);
        timerTaskMapping.put(timerId, taskKey);
        taskTimerMapping.put(taskKey, timerId);
    }

    public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment)
    {
        final TypedEventProcessor<TaskEvent> registerTask = new TypedEventProcessor<TaskEvent>()
        {
            @Override
            public void updateState(TypedEvent<TaskEvent> event)
            {
                final long lockTime = event.getValue().getLockTime();

                mapAccessBuffer.putLong(0, event.getPosition());
                mapAccessBuffer.putLong(SIZE_OF_LONG, lockTime);

                expirationMap.put(event.getKey(), mapAccessBuffer);

                addTaskToWheel(event.getKey(), lockTime);
            }
        };

        final TypedEventProcessor<TaskEvent> unregisterTask = new TypedEventProcessor<TaskEvent>()
        {
            @Override
            public void updateState(TypedEvent<TaskEvent> event)
            {
                expirationMap.remove(event.getKey());
                final long timerId = taskTimerMapping.get(event.getKey());
                if (timerId != -1)
                {
                    timerWheel.cancelTimer(timerId);
                }
            }
        };

        return environment.newStreamProcessor()
            .onEvent(EventType.TASK_EVENT, TaskState.LOCKED, registerTask)
            .onEvent(EventType.TASK_EVENT, TaskState.LOCK_EXPIRED, unregisterTask)
            .onEvent(EventType.TASK_EVENT, TaskState.COMPLETED, unregisterTask)
            .onEvent(EventType.TASK_EVENT, TaskState.FAILED, unregisterTask)
            .withListener(streamWriter)
            .withListener(this)
            .withStateResource(expirationMap)
            .build();
    }
}
