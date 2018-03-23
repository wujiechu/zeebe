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
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReactiveStreamProcessorController extends Actor
{
    private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

    private static final int EVENT_WINDIW_SIZE = 128;
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


    private final EnumMap<EventType, ReusableObjectList<? extends UnpackedObject>> eventPools;
    private final ReusableObjectList<EventRef> eventWindow;
    private final EnumMap<EventType, List<StreamProcessorController>> typedControllers;

    private ActorCondition actorCondition;

    public ReactiveStreamProcessorController(StreamProcessorContext context)
    {
        this.streamProcessorContext = context;

        this.actorScheduler = context.getActorScheduler();
        this.logStreamReader = context.getLogStreamReader();

        brokerEventMetadata = new BrokerEventMetadata();

        this.eventPools = new EnumMap<>(EventType.class);
        initPools();
        eventWindow = new ReusableObjectList<>(() -> new EventRef(this::releaseEvent));
        typedControllers = new EnumMap<EventType, List<StreamProcessorController>>(EventType.class);
    }

    private void initPools()
    {
        EVENT_REGISTRY.forEach((t, c) ->
            {
                eventPools.put(t, new ReusableObjectList<>(() -> ReflectUtil.newInstance(c)));
            }
        );
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

            eventWindow.remove(eventRef);
            actor.submit(this::processEvent);
        });
    }

    @Override
    public String getName()
    {
        return streamProcessorContext.getName();
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
            }

            streamProcessorControllers.add(controller);
        });
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
        if (eventWindow.size() < EVENT_WINDIW_SIZE)
        {

            if (logStreamReader.hasNext())
            {
                final LoggedEvent next = logStreamReader.next();
                next.readMetadata(brokerEventMetadata);

                final EventType eventType = brokerEventMetadata.getEventType();

                LOG.debug("Process next event of type {} on position {}", eventType, next.getPosition());

                final ReusableObjectList<? extends UnpackedObject> eventPool = eventPools.get(eventType);
                if (eventPool.size() < EVENT_WINDIW_SIZE)
                {
                    final List<StreamProcessorController> streamProcessorControllers = typedControllers.get(eventType);

                    final UnpackedObject event = eventPool.add();
                    next.readValue(event);


                    final EventRef eventRef = eventWindow.add();
                    eventRef.setEvent(event);
                    eventRef.setType(eventType);
                    eventRef.setRefCount(streamProcessorControllers.size());
                    eventRef.setPosition(next.getPosition());


                    LOG.debug("Distribute unpacked event to {} registered controller", streamProcessorControllers.size());
                    streamProcessorControllers.forEach((s) -> s.hintEvent(eventRef));

                    actor.submit(this::processEvent);
                }
                else
                {
                    LOG.debug("Event pool for event of type {} is exhausted.", eventType);
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
