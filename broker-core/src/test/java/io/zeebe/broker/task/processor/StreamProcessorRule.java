package io.zeebe.broker.task.processor;

import java.util.function.Function;

import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.broker.topic.TestStreams;
import io.zeebe.broker.transport.clientapi.BufferingServerOutput;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.util.sched.clock.ControlledActorClock;
import io.zeebe.util.sched.testing.ActorSchedulerRule;

public class StreamProcessorRule implements TestRule
{


    // environment
    private TemporaryFolder tempFolder = new TemporaryFolder();
    private AutoCloseableRule closeables = new AutoCloseableRule();
    private ControlledActorClock clock = new ControlledActorClock();
    private ActorSchedulerRule actorSchedulerRule = new ActorSchedulerRule(clock);

    // things provisioned by this rule
    public static final String STREAM_NAME = "stream";

    private BufferingServerOutput output;
    private TestStreams streams;
    private TypedStreamEnvironment streamEnvironment;

    private SetupRule rule = new SetupRule();

    private RuleChain chain = RuleChain
            .outerRule(tempFolder)
            .around(actorSchedulerRule)
            .around(closeables)
            .around(rule);

    @Override
    public Statement apply(Statement base, Description description)
    {
        return chain.apply(base, description);
    }

    public StreamProcessorControl runStreamProcessor(Function<TypedStreamEnvironment, StreamProcessor> factory)
    {
        final StreamProcessorControl control = initStreamProcessor(factory);
        control.start();
        return control;
    }

    public StreamProcessorControl initStreamProcessor(Function<TypedStreamEnvironment, StreamProcessor> factory)
    {
        return streams.initStreamProcessor(STREAM_NAME, 0, () -> factory.apply(streamEnvironment));
    }

    public ControlledActorClock getClock()
    {
        return clock;
    }

    public TypedEventStream events()
    {
        return new TypedEventStream(streams.events(STREAM_NAME));
    }

    public long writeEvent(long key, UnpackedObject value)
    {
        return streams.newEvent(STREAM_NAME)
            .key(key)
            .event(value)
            .write();
    }

    public long writeEvent(UnpackedObject value)
    {
        return streams.newEvent(STREAM_NAME)
            .event(value)
            .write();
    }

    private class SetupRule extends ExternalResource
    {

        @Override
        protected void before() throws Throwable
        {
            output = new BufferingServerOutput();

            streams = new TestStreams(tempFolder.getRoot(), closeables, actorSchedulerRule.get());
            streams.createLogStream(STREAM_NAME);

            streams.newEvent(STREAM_NAME) // TODO: workaround for https://github.com/zeebe-io/zeebe/issues/478
                .event(new UnpackedObject())
                .write();

            streamEnvironment = new TypedStreamEnvironment(streams.getLogStream(STREAM_NAME), output);
        }
    }

}
