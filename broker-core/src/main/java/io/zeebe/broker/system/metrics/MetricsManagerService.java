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
        final Map<String, String> globalLabels = new HashMap<>();
        globalLabels.put("cluster", "meintollercluster");
        globalLabels.put("node", "weißichdochauchnicht");
        metricsManager = new MetricsManager(globalLabels);

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
