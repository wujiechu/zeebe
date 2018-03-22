package io.zeebe.broker.system.metrics;

import java.util.HashMap;
import java.util.Map;

import io.zeebe.servicecontainer.*;
import io.zeebe.util.metrics.Metric;
import io.zeebe.util.metrics.MetricsManager;

public class MetricsManagerService implements Service<MetricsManager>
{
    private MetricsManager metricsManager;
    private Metric brokerInfoMetric;

    @Override
    public MetricsManager get()
    {
        return metricsManager;
    }

    @Override
    public void start(ServiceStartContext ctx)
    {
        metricsManager = new MetricsManager();

        final Map<String, String> labels = new HashMap<>();
        labels.put("version", "0.8.0-SNAPSHOT");
        brokerInfoMetric = metricsManager.allocate("zb_broker_info", labels);
        brokerInfoMetric.incrementOrdered();
    }

    @Override
    public void stop(ServiceStopContext ctx)
    {
        brokerInfoMetric.close();
    }
}
