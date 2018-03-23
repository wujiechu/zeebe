/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.logstreams.processor.reactive;

import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.processor.EventFilter;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamProcessorController extends Actor
{
    private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

    private static final String ERROR_MESSAGE_REPROCESSING_FAILED = "Stream processor '%s' failed to reprocess. Cannot find source event position: %d";
    private static final String ERROR_MESSAGE_PROCESSING_FAILED = "Stream processor '{}' failed to process event. It stop processing further events.";

    private final StreamProcessor streamProcessor;
    private final StreamProcessorContext streamProcessorContext;

    private final LogStreamWriter logStreamWriter;

    private final ActorScheduler actorScheduler;
    private final AtomicBoolean isOpened = new AtomicBoolean(false);
    private final AtomicBoolean isFailed = new AtomicBoolean(false);

    private final EventFilter eventFilter;

    private final Runnable nextEvent = this::nextEvent;

    private long eventPosition = -1L;

    private EventProcessor eventProcessor;
    private ActorCondition onCommitPositionUpdatedCondition;

    private boolean suspended = false;

    private final Deque<EventRef> queue = new ArrayDeque<>();
    private EventRef currentEvent;
    private long lastSuccessfulProcessedEventPosition;
    private long lastWrittenEventPosition;

    public StreamProcessorController(StreamProcessorContext context)
    {
        this.streamProcessorContext = context;

        this.actorScheduler = context.getActorScheduler();
        this.streamProcessor = context.getStreamProcessor();
        this.logStreamWriter = context.getLogStreamWriter();
        this.eventFilter = context.getEventFilter();
    }


    public void hintEvent(EventRef eventRef)
    {
        LOG.debug("Got event hint {}", eventRef);
        actor.call(() ->
        {
            LOG.debug("Offer event {} to queue ", eventRef.getPosition());
            queue.offer(eventRef);
//            streamProcessor.nextEvent(type, eventRef);
            actor.submit(nextEvent);
        });
    }

    @Override
    public String getName()
    {
        return streamProcessorContext.getName();
    }

    public ActorFuture<Void> openAsync()
    {
        if (isOpened.compareAndSet(false, true))
        {
            return actorScheduler.submitActor(this);
        }
        else
        {
            return CompletableActorFuture.completed(null);
        }
    }

    @Override
    protected void onActorStarted()
    {
        final LogStream logStream = streamProcessorContext.getLogStream();
        logStreamWriter.wrap(logStream);
    }

    private void nextEvent()
    {
        if (isOpened() && !isSuspended())
        {
            final EventRef eventRef = queue.poll();
            currentEvent = eventRef;

            if (eventRef != null)
            {
                processEvent(eventRef);
            }
            else
            {
                actor.yield();
            }
        }
        else
        {
            actor.yield();
        }
    }

    private void processEvent(EventRef eventRef)
    {
        eventProcessor = streamProcessor.onEvent(eventRef);

        if (eventProcessor != null)
        {
            try
            {
                eventProcessor.processEvent();

                actor.runUntilDone(this::executeSideEffects);
            }
            catch (Exception e)
            {
                LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);
                onFailure();
            }
        }
    }

    private void executeSideEffects()
    {
        try
        {
            final boolean success = eventProcessor.executeSideEffects();
            if (success)
            {
                actor.done();

                actor.runUntilDone(this::writeEvent);
            }
            else if (isOpened())
            {
                // try again
                actor.yield();
            }
            else
            {
                actor.done();
            }
        }
        catch (Exception e)
        {
            LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);

            actor.done();
            onFailure();
        }
    }

    private void writeEvent()
    {
        try
        {
            final LogStream sourceStream = streamProcessorContext.getLogStream();

            logStreamWriter
                .producerId(streamProcessorContext.getId())
                .sourceEvent(sourceStream.getPartitionId(), currentEvent.getPosition());

            eventPosition = eventProcessor.writeEvent(logStreamWriter);

            if (eventPosition >= 0)
            {
                actor.done();

                updateState();
            }
            else if (isOpened())
            {
                // try again
                actor.yield();
            }
            else
            {
                actor.done();
            }
        }
        catch (Exception e)
        {
            LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);

            actor.done();
            onFailure();
        }
    }

    private void updateState()
    {
        try
        {
            eventProcessor.updateState();
            streamProcessor.afterEvent();

            lastSuccessfulProcessedEventPosition = currentEvent.getPosition();

            final boolean hasWrittenEvent = eventPosition > 0;
            if (hasWrittenEvent)
            {
                lastWrittenEventPosition = eventPosition;
            }
        }
        catch (Exception e)
        {
            LOG.error(ERROR_MESSAGE_PROCESSING_FAILED, getName(), e);
            onFailure();
        }
    }

    public ActorFuture<Void> closeAsync()
    {
        if (isOpened.compareAndSet(true, false))
        {
            return actor.close();
        }
        else
        {
            return CompletableActorFuture.completed(null);
        }
    }

    @Override
    protected void onActorClosing()
    {
        if (!isFailed())
        {
            streamProcessor.onClose();
        }

        streamProcessorContext.getLogStreamReader().close();

        streamProcessorContext.logStream.removeOnCommitPositionUpdatedCondition(onCommitPositionUpdatedCondition);
        onCommitPositionUpdatedCondition = null;
    }

    private void onFailure()
    {
        if (isFailed.compareAndSet(false, true))
        {
            isOpened.set(false);

            actor.close();
        }
    }

    public boolean isOpened()
    {
        return isOpened.get();
    }

    public boolean isFailed()
    {
        return isFailed.get();
    }

    public EventFilter getEventFilter()
    {
        return eventFilter;
    }


    public boolean isSuspended()
    {
        return suspended;
    }

    public StreamProcessor getStreamProcessor()
    {
        return streamProcessor;
    }
}
