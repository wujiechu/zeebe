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

import io.zeebe.logstreams.log.*;
import io.zeebe.logstreams.processor.EventFilter;
import io.zeebe.util.sched.ActorScheduler;

import java.util.Objects;

public class StreamContextBuilder
{
    protected int id;
    protected String name;

    protected StreamProcessor streamProcessor;

    protected LogStream logStream;

    protected ActorScheduler actorScheduler;


    protected LogStreamReader logStreamReader;
    protected LogStreamWriter logStreamWriter;

    protected EventFilter eventFilter;

    public StreamContextBuilder(int id, String name, StreamProcessor streamProcessor)
    {
        this.id = id;
        this.name = name;
        this.streamProcessor = streamProcessor;
    }

    public StreamContextBuilder logStream(LogStream stream)
    {
        this.logStream = stream;
        return this;
    }

    public StreamContextBuilder actorScheduler(ActorScheduler actorScheduler)
    {
        this.actorScheduler = actorScheduler;
        return this;
    }

    /**
     * @param eventFilter may be null to accept all events
     */
    public StreamContextBuilder eventFilter(EventFilter eventFilter)
    {
        this.eventFilter = eventFilter;
        return this;
    }

    protected void initContext()
    {
        Objects.requireNonNull(streamProcessor, "No stream processor provided.");
        Objects.requireNonNull(logStream, "No log stream provided.");
        Objects.requireNonNull(actorScheduler, "No task scheduler provided.");

        logStreamReader = new BufferedLogStreamReader();

        logStreamWriter = new LogStreamWriterImpl();
    }

    public StreamProcessorContext build()
    {
        initContext();

        final StreamProcessorContext ctx = new StreamProcessorContext();

        ctx.setId(id);
        ctx.setName(name);

        ctx.setStreamProcessor(streamProcessor);

        ctx.setLogStream(logStream);

        ctx.setActorScheduler(actorScheduler);

        ctx.setLogStreamReader(logStreamReader);
        ctx.setLogStreamWriter(logStreamWriter);


        ctx.setEventFilter(eventFilter);

        return ctx;
    }
}
