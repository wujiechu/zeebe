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

import io.zeebe.broker.incident.data.IncidentEvent;
import io.zeebe.broker.system.log.PartitionEvent;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.workflow.data.DeploymentEvent;
import io.zeebe.broker.workflow.data.WorkflowEvent;
import io.zeebe.broker.workflow.data.WorkflowInstanceEvent;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.util.ReflectUtil;
import io.zeebe.util.collection.ReusableObjectList;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.SchedulingHints;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReactiveStreamProcessorController extends Actor
{
    private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

    private static final int EVENT_WINDIW_SIZE = 8;
    private static final EnumMap<EventType, Class<? extends UnpackedObject>> EVENT_REGISTRY = new EnumMap<>(EventType.class);
    static
    {
        EVENT_REGISTRY.put(EventType.TOPIC_EVENT, TopicEvent.class);
        EVENT_REGISTRY.put(EventType.TASK_EVENT, TaskEvent.class);
        EVENT_REGISTRY.put(EventType.PARTITION_EVENT, PartitionEvent.class);
        EVENT_REGISTRY.put(EventType.DEPLOYMENT_EVENT, DeploymentEvent.class);
        EVENT_REGISTRY.put(EventType.WORKFLOW_EVENT, WorkflowEvent.class);
        EVENT_REGISTRY.put(EventType.WORKFLOW_INSTANCE_EVENT, WorkflowInstanceEvent.class);
        EVENT_REGISTRY.put(EventType.INCIDENT_EVENT, IncidentEvent.class);
    }

    private final StreamProcessorContext streamProcessorContext;
    private final ActorScheduler actorScheduler;

    private final LogStreamReader logStreamReader;

    private final AtomicBoolean isOpened = new AtomicBoolean(false);
    private final BrokerEventMetadata brokerEventMetadata;


    private final EnumMap<EventType, OneToOneConcurrentArrayQueue<UnpackedObject>> eventPools;
    private final OneToOneConcurrentArrayQueue<EventRef> eventWindow;
    private final EnumMap<EventType, List<StreamProcessorController>> typedControllers;

    private ActorCondition actorCondition;

    public ReactiveStreamProcessorController(StreamProcessorContext context)
    {
        this.streamProcessorContext = context;

        this.actorScheduler = context.getActorScheduler();
        this.logStreamReader = context.getLogStreamReader();

        brokerEventMetadata = new BrokerEventMetadata();

        this.eventPools = new EnumMap<>(EventType.class);

        this.eventWindow = new OneToOneConcurrentArrayQueue<>(EVENT_WINDIW_SIZE);
        for (int i = 0; i < EVENT_WINDIW_SIZE; i++)
        {
            this.eventWindow.offer(new EventRef(this::releaseEvent));
        }
        this.typedControllers = new EnumMap<EventType, List<StreamProcessorController>>(EventType.class);
    }


    /**
     *  Called internally by event ref, if ref count is equal to zero.
     *  EventRef will be reseted and returned to "pool". In that case we can restart processing the next event, if
     *  available.
     *
     * @param eventRef
     */
    private void releaseEvent(EventRef eventRef)
    {
        actor.call(() ->
        {
            LOG.debug("Release event ref {} and trigger new processing.", eventRef);

            eventPools.get(eventRef.getType()).offer(eventRef.getEvent());
            eventWindow.offer(eventRef);
            eventRef.reset();

            actor.run(this::processEvent);
        });
    }

    @Override
    public String getName()
    {
        return "ReactiveStreamProcessor";
    }

    public ActorFuture<Void> registerForEvent(EventType eventType, StreamProcessorController controller)
    {
        return actor.call(() ->
        {
            LOG.debug("Register {} for event type {}", controller, eventType);

            List<StreamProcessorController> streamProcessorControllers = typedControllers.get(eventType);

            if (streamProcessorControllers == null)
            {
                streamProcessorControllers = new ArrayList<>();
                typedControllers.put(eventType, streamProcessorControllers);

                OneToOneConcurrentArrayQueue<UnpackedObject> eventPool = new OneToOneConcurrentArrayQueue<>(EVENT_WINDIW_SIZE);
                for (int i = 0; i < EVENT_WINDIW_SIZE; i++)
                {
                    eventPool.offer(ReflectUtil.newInstance(EVENT_REGISTRY.get(eventType)));
                }
                eventPools.put(eventType, eventPool);
            }

            streamProcessorControllers.add(controller);
        });
    }

    public ActorFuture<Void> openAsync()
    {
        if (isOpened.compareAndSet(false, true))
        {
            return actorScheduler.submitActor(this, false, SchedulingHints.ioBound((short) 0));
        }
        else
        {
            return CompletableActorFuture.completed(null);
        }
    }

    public ActorFuture<Void> closeAsync()
    {
        return actor.close();
    }

    @Override
    protected void onActorStarted()
    {
        final LogStream logStream = streamProcessorContext.getLogStream();
        logStreamReader.wrap(logStream);

        this.actorCondition = actor.onCondition("on-event-commited", this::processEvent);
        logStream.registerOnCommitPositionUpdatedCondition(actorCondition);

        LOG.debug("{} started", ReactiveStreamProcessorController.class.getName());
    }


    private void processEvent()
    {
        LOG.debug("Reactive event processing");
        /*
         * [              WINDOW                ]
         * [ (Event|Count) | ....               ]
         */
        if (!eventWindow.isEmpty())
        {

            if (logStreamReader.hasNext())
            {
                final LoggedEvent next = logStreamReader.next();
                next.readMetadata(brokerEventMetadata);

                final EventType eventType = brokerEventMetadata.getEventType();

                LOG.debug("Process next event of type {} on position {}", eventType, next.getPosition());

                final OneToOneConcurrentArrayQueue<UnpackedObject> eventPool = eventPools.get(eventType);
                if (!eventPool.isEmpty())
                {
                    final List<StreamProcessorController> streamProcessorControllers = typedControllers.get(eventType);

                    final UnpackedObject event = eventPool.poll();
                    next.readValue(event);

                    final EventRef eventRef = eventWindow.poll();
                    eventRef.setEvent(event);
                    eventRef.setType(eventType);
                    eventRef.setRefCount(streamProcessorControllers.size());
                    eventRef.setPosition(next.getPosition());

                    LOG.debug("Distribute unpacked event to {} registered controller", streamProcessorControllers.size());
                    streamProcessorControllers.forEach((s) -> s.hintEvent(eventRef));

                    actor.run(this::processEvent);
                }
                else
                {
                    // TODO we need to step back - so we read this event next time again
                    LOG.error("Event pool for event of type {} is exhausted, size is {}", eventType, eventPool.size());
                }
            }
            else
            {
                LOG.debug("No next event to process.");
            }
        }
        else
        {
            LOG.debug("Capacity of event window reached.");
        }
    }


    /**
     * Reprocessing is done on each stream process controller.
     * After stream process controller is finished he tells the master the last event position he has processed.
     *
     * The master uses these positions to calculate the events window.
     */

}
