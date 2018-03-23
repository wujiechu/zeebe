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
package io.zeebe.topic;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.zeebe.client.ClientProperties;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.impl.ZeebeClientImpl;

public class Sam
{

    public static void main(final String[] args) throws InterruptedException, ExecutionException
    {
        final String brokerContactPoint = "127.0.0.1:51015";

        final Properties clientProperties = new Properties();
        clientProperties.put(ClientProperties.BROKER_CONTACTPOINT, brokerContactPoint);
        clientProperties.put(ClientProperties.CLIENT_TOPIC_SUBSCRIPTION_PREFETCH_CAPACITY, "128");

        try (ZeebeClient client = new ZeebeClientImpl(clientProperties))
        {

            System.out.println(String.format("> Connecting to %s", brokerContactPoint));

            final String topicName = UUID.randomUUID().toString();

            client.topics().create(topicName, 1).execute();
            client.workflows().deploy(topicName).addResourceFromClasspath("demoProcess.bpmn").execute();

            final int instances = 20_000;
            final int maxRequests = 100;
            final List<Future<?>> futures = new ArrayList<>(maxRequests);

            final CountDownLatch latch = new CountDownLatch(instances);

            client.tasks()
                  .newTaskSubscription(topicName)
                  .taskType("foo")
                  .lockOwner("foo")
                  .lockTime(30_000)
                  .taskFetchSize(30)
                  .handler((c, task) -> latch.countDown())
                  .open();

            long start = System.currentTimeMillis();
            long created = 0;
            while (created < instances)
            {
                while (futures.size() < maxRequests)
                {
                    futures.add(client.workflows().create(topicName).bpmnProcessId("demoProcess").payload("{\"a\": \"b\"}").executeAsync());
                }

                final Iterator<Future<?>> iterator = futures.iterator();
                while (iterator.hasNext())
                {
                    final Future<?> future = iterator.next();
                    if (future.isDone())
                    {
                        future.get();
                        iterator.remove();
                        created++;
                    }
                }
            }
            long end = System.currentTimeMillis();

            System.out.println("Created " + instances + " workflow instances in " + (end - start) + "ms");


            start = System.currentTimeMillis();
            latch.await();
            end = System.currentTimeMillis();

            System.out.println("Took " + (end - start) + "ms to complete " + instances + " workflow instances");
        }

    }

}
