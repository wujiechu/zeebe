package io.zeebe.broker.task.processor;

import java.util.stream.Stream;

import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedEventImpl;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.topic.Events;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.util.ReflectUtil;

public class TypedEventStream extends StreamWrapper<LoggedEvent>
{

    public TypedEventStream(Stream<LoggedEvent> stream)
    {
        super(stream);
    }

    public TaskEventStream onlyTaskEvents()
    {
        return new TaskEventStream(
            filter(Events::isTaskEvent)
            .map(e -> toTypedEvent(e, TaskEvent.class)));
    }

    private <T extends UnpackedObject> TypedEvent<T> toTypedEvent(LoggedEvent event, Class<T> valueClass)
    {
        final T value = ReflectUtil.newInstance(valueClass);
        value.wrap(event.getValueBuffer(), event.getValueOffset(), event.getValueLength());
        return new CopiedTypedEvent(event, value);
    }

    private class CopiedTypedEvent extends TypedEventImpl
    {
        private final long key;
        private final long position;

        CopiedTypedEvent(LoggedEvent event, UnpackedObject object)
        {
            this.value = object;
            this.position = event.getPosition();
            this.key = event.getKey();
        }

        @Override
        public long getKey()
        {
            return key;
        }

        @Override
        public long getPosition()
        {
            return position;
        }

        @Override
        public BrokerEventMetadata getMetadata()
        {
            throw new UnsupportedOperationException("not implemented yet; be the change you want to see in the world");
        }
    }

}
