package io.zeebe.broker.system.metrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;

import io.zeebe.broker.Loggers;
import io.zeebe.util.FileUtil;
import io.zeebe.util.metrics.MetricsManager;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.ExpandableDirectByteBuffer;
import org.slf4j.Logger;

public class MetricsFileWriter extends Actor
{
    private static final Logger LOG = Loggers.SYSTEM_LOGGER;

    private final MetricsManager metricsManager;
    private final Duration reportingInterval;
    private final String filePath;
    private final ExpandableDirectByteBuffer writeBuffer = new ExpandableDirectByteBuffer();
    private FileChannel fileChannel = null;

    public MetricsFileWriter(Duration reportingInterval, String filePath, MetricsManager metricsManager)
    {
        this.reportingInterval = reportingInterval;
        this.filePath = filePath;
        this.metricsManager = metricsManager;
    }

    @Override
    protected void onActorStarting()
    {
        LOG.debug("Writing metrics to file {}. Log interval {}s.", filePath, reportingInterval.toMillis() / 1000);
        fileChannel = FileUtil.openChannel(filePath, true);
    }

    @Override
    protected void onActorStarted()
    {
        actor.runAtFixedRate(reportingInterval, this::dump);
    }

    private void dump()
    {
        final int length = metricsManager.dump(writeBuffer, 0);
        final ByteBuffer inBuffer = writeBuffer.byteBuffer();
        inBuffer.position(0);
        inBuffer.limit(length);

        try
        {
            fileChannel.position(0);
            fileChannel.truncate(length);

            while (inBuffer.hasRemaining())
            {
                fileChannel.write(inBuffer);
            }

            fileChannel.force(false);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    protected void onActorClosing()
    {
        FileUtil.closeSilently(fileChannel);
    }

    public ActorFuture<Void> close()
    {
        return actor.close();
    }
}
