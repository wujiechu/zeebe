package io.zeebe.broker.logstreams.processor.reactive;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.util.collection.Reusable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public final class EventRef implements Reusable
{
    private final Consumer<EventRef> resetCallback;

    private EventType type;
    private AtomicInteger refCount;
    private long position;
    private UnpackedObject event;

    public EventRef(Consumer<EventRef> resetCallback)
    {
        this.resetCallback = resetCallback;
        this.refCount = new AtomicInteger(-1);
    }

    public void setRefCount(int refCount)
    {
        this.refCount.set(refCount);
    }

    public void setEvent(UnpackedObject event)
    {
        this.event = event;
    }

    public EventType getType()
    {
        return type;
    }

    public void setType(EventType type)
    {
        this.type = type;
    }

    public long getPosition()
    {
        return position;
    }

    public void setPosition(long position)
    {
        this.position = position;
    }

    public void copyEvent(UnpackedObject unpackedObject)
    {
        event.copy(unpackedObject);

        if (refCount.decrementAndGet() == 0)
        {
            resetCallback.accept(this);
        }

    }

    @Override
    public String toString()
    {
        return "EventRef{" +
            ", type=" + type +
            ", refCount=" + refCount +
            ", position=" + position +
            '}';
    }

    @Override
    public void reset()
    {
        refCount.set(-1);
        position = -1;
        type = null;
        event = null;
    }
}
