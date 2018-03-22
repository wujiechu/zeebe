package io.zeebe.broker.system.metrics;

import java.io.File;
import java.time.Duration;

import io.zeebe.broker.system.metrics.cfg.MetricsCfg;
import io.zeebe.servicecontainer.*;
import io.zeebe.util.metrics.MetricsManager;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.SchedulingHints;

public class MetricsFileWriterService implements Service<MetricsFileWriter>
{
    private final Injector<MetricsManager> metricsManagerInjector = new Injector<>();
    private MetricsFileWriter metricsFileWriter;
    private MetricsCfg cfg;

    public MetricsFileWriterService(MetricsCfg cfg)
    {
        this.cfg = cfg;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        final ActorScheduler scheduler = startContext.getScheduler();
        final MetricsManager metricsManager = metricsManagerInjector.getValue();

        final String metricsFileName = new File(cfg.getDirectory(), "zeebe.prom").getAbsolutePath();

        metricsFileWriter = new MetricsFileWriter(Duration.ofSeconds(5), metricsFileName, metricsManager);
        startContext.async(scheduler.submitActor(metricsFileWriter, SchedulingHints.isIoBound(0)));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(metricsFileWriter.close());
    }

    @Override
    public MetricsFileWriter get()
    {
        return metricsFileWriter;
    }

    public Injector<MetricsManager> getMetricsManagerInjector()
    {
        return metricsManagerInjector;
    }

}
