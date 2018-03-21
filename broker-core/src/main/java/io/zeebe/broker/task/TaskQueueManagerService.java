/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.task;

import static io.zeebe.broker.logstreams.LogStreamServiceNames.SNAPSHOT_STORAGE_SERVICE;
import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.TASK_EXPIRE_LOCK_STREAM_PROCESSOR_ID;
import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.TASK_QUEUE_STREAM_PROCESSOR_ID;
import static io.zeebe.broker.task.TaskQueueServiceNames.TASK_QUEUE_STREAM_PROCESSOR_SERVICE_GROUP_NAME;
import static io.zeebe.broker.task.TaskQueueServiceNames.taskQueueExpireLockStreamProcessorServiceName;
import static io.zeebe.broker.task.TaskQueueServiceNames.taskQueueInstanceStreamProcessorServiceName;

import java.time.Duration;

import io.zeebe.broker.logstreams.processor.StreamProcessorService;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.task.processor.TaskExpireLockStreamProcessor;
import io.zeebe.broker.task.processor.TaskExpireLockStreamProcessor2;
import io.zeebe.broker.task.processor.TaskInstanceStreamProcessor;
import io.zeebe.broker.task.processor.streamProcessor;
import io.zeebe.broker.transport.clientapi.CommandResponseWriter;
import io.zeebe.broker.transport.clientapi.SubscribedEventWriter;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.processor.StreamProcessorController;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorScheduler;

public class TaskQueueManagerService implements Service<TaskQueueManager>, TaskQueueManager
{
    protected static final String NAME = "task.queue.manager";
    public static final Duration LOCK_EXPIRATION_INTERVAL = Duration.ofSeconds(30);

    protected final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
    protected final Injector<TaskSubscriptionManager> taskSubscriptionManagerInjector = new Injector<>();

    protected final ServiceGroupReference<LogStream> logStreamsGroupReference = ServiceGroupReference.<LogStream>create()
            .onAdd((name, stream) -> addStream(name, stream))
            .build();

    private ServiceStartContext serviceContext;
    private ActorScheduler actorScheduler;

    @Override
    public void startTaskQueue(ServiceName<LogStream> streamName, LogStream stream)
    {

        System.out.println("Installing stream processor in log " + streamName);

        final ServiceName<StreamProcessorController> streamProcessorServiceName = taskQueueInstanceStreamProcessorServiceName(stream.getLogName());
        final String streamProcessorName = streamProcessorServiceName.getName();

        final ServerTransport serverTransport = clientApiTransportInjector.getValue();

        final CommandResponseWriter responseWriter = new CommandResponseWriter(serverTransport.getOutput());
        final SubscribedEventWriter subscribedEventWriter = new SubscribedEventWriter(serverTransport.getOutput());
        final TaskSubscriptionManager taskSubscriptionManager = taskSubscriptionManagerInjector.getValue();

        final TaskInstanceStreamProcessor taskInstanceStreamProcessor = new TaskInstanceStreamProcessor(responseWriter, subscribedEventWriter, taskSubscriptionManager);
        final StreamProcessorService taskInstanceStreamProcessorService = new StreamProcessorService(
                streamProcessorName,
                TASK_QUEUE_STREAM_PROCESSOR_ID,
                taskInstanceStreamProcessor)
                .eventFilter(TaskInstanceStreamProcessor.eventFilter());

        serviceContext.createService(streamProcessorServiceName, taskInstanceStreamProcessorService)
              .group(TASK_QUEUE_STREAM_PROCESSOR_SERVICE_GROUP_NAME)
              .dependency(streamName, taskInstanceStreamProcessorService.getLogStreamInjector())
              .dependency(SNAPSHOT_STORAGE_SERVICE, taskInstanceStreamProcessorService.getSnapshotStorageInjector())
              .install();

        startExpireLockService(streamName, stream);
    }

    protected void startExpireLockService(ServiceName<LogStream> streamName, LogStream stream)
    {

        final ServerTransport serverTransport = clientApiTransportInjector.getValue();

        final TypedStreamEnvironment environment = new TypedStreamEnvironment(stream, serverTransport.getOutput());

        final TaskExpireLockStreamProcessor2 processor = new TaskExpireLockStreamProcessor2(environment.buildStreamReader(), environment.buildStreamWriter());
        final TypedStreamProcessor streamProcessor = processor.createStreamProcessor(environment);

        final ServiceName<StreamProcessorController> expireLockStreamProcessorServiceName = taskQueueExpireLockStreamProcessorServiceName(stream.getLogName());
//        final TaskExpireLockStreamProcessor expireLockStreamProcessor = new TaskExpireLockStreamProcessor();

        final StreamProcessorService expireLockStreamProcessorService = new StreamProcessorService(
                expireLockStreamProcessorServiceName.getName(),
                TASK_EXPIRE_LOCK_STREAM_PROCESSOR_ID,
                streamProcessor)
                .eventFilter(TaskExpireLockStreamProcessor.eventFilter());

        serviceContext.createService(expireLockStreamProcessorServiceName, expireLockStreamProcessorService)
            .dependency(streamName, expireLockStreamProcessorService.getLogStreamInjector())
            .dependency(SNAPSHOT_STORAGE_SERVICE, expireLockStreamProcessorService.getSnapshotStorageInjector())
            .install();
    }

    @Override
    public void start(ServiceStartContext serviceContext)
    {
        this.serviceContext = serviceContext;

        actorScheduler = serviceContext.getScheduler();
    }

    @Override
    public void stop(ServiceStopContext ctx)
    {
    }

    @Override
    public TaskQueueManager get()
    {
        return this;
    }

    public Injector<ServerTransport> getClientApiTransportInjector()
    {
        return clientApiTransportInjector;
    }

    public Injector<TaskSubscriptionManager> getTaskSubscriptionManagerInjector()
    {
        return taskSubscriptionManagerInjector;
    }

    public ServiceGroupReference<LogStream> getLogStreamsGroupReference()
    {
        return logStreamsGroupReference;
    }

    public void addStream(ServiceName<LogStream> streamName, LogStream logStream)
    {
        actorScheduler.submitActor(new Actor()
        {
            @Override
            protected void onActorStarted()
            {
                startTaskQueue(streamName, logStream);
            }
        });
    }

}
