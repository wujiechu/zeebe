package io.zeebe.broker.logstreams.processor.reactive;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.util.collection.Reusable;

import java.util.function.Consumer;

public final class EventRef implements Reusable
{
    private final Consumer<EventRef> resetCallback;

    private EventType type;
    private int refCount;
    private UnpackedObject event;

    public EventRef(Consumer<EventRef> resetCallback)
    {
        this.resetCallback = resetCallback;
    }

    public void setRefCount(int refCount)
    {
        this.refCount = refCount;
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

    public void copyEvent(UnpackedObject unpackedObject)
    {
        event.copy(unpackedObject);

        refCount--;

        if (refCount == 0)
        {
            resetCallback.accept(this);
        }

    }

    @Override
    public void reset()
    {
        type = null;
        refCount = -1;
        event = null;
    }
}
