package io.zeebe.broker.clustering.raft;

import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftApiMessageHandler;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;

public class RaftApiMessageHandlerService implements Service<RaftApiMessageHandler>
{

    protected RaftApiMessageHandler service;

    protected final ServiceGroupReference<Raft> raftGroupReference = ServiceGroupReference.<Raft>create()
        .onAdd((name, raft) -> service.registerRaft(raft))
        .onRemove((name, raft) -> service.removeRaft(raft))
        .build();

    @Override
    public void start(ServiceStartContext startContext)
    {
        service = new RaftApiMessageHandler();
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        // nothing to do
    }

    @Override
    public RaftApiMessageHandler get()
    {
        return service;
    }

    public ServiceGroupReference<Raft> getRaftGroupReference()
    {
        return raftGroupReference;
    }

}
