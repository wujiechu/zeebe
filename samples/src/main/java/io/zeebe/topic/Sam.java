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

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import io.zeebe.client.ClientProperties;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.impl.ZeebeClientImpl;

public class Sam
{

    public static void main(final String[] args) throws InterruptedException
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

            final int instances = 3_000;

            long start = System.currentTimeMillis();
            for (int i = 0; i < instances; i++)
            {
                client.workflows().create(topicName).bpmnProcessId("demoProcess").payload("{\"a\": \"b\"}").execute();
            }
            long end = System.currentTimeMillis();

            System.out.println("Created " + instances + " workflow instances in " + (end - start) + "ms");

            client.tasks()
                  .newTaskSubscription(topicName)
                  .taskType("foo")
                  .lockOwner("foo")
                  .lockTime(30_000)
                  .taskFetchSize(30)
                  .handler((c, task) -> c.complete(task).payload(task.getPayload()).execute())
                  .open();

            client.tasks()
                  .newTaskSubscription(topicName)
                  .taskType("bar")
                  .lockOwner("bar")
                  .lockTime(30_000)
                  .taskFetchSize(30)
                  .handler((c, task) -> c.complete(task).execute())
                  .open();

            client.tasks()
                  .newTaskSubscription(topicName)
                  .taskType("foobar")
                  .lockOwner("foobar")
                  .lockTime(30_000)
                  .taskFetchSize(30)
                  .handler((c, task) -> c.complete(task).execute())
                  .open();

            final CountDownLatch latch = new CountDownLatch(instances);

            client.topics()
                  .newSubscription(topicName)
                  .name("counter")
                  .startAtHeadOfTopic()
                  .forcedStart()
                  .workflowInstanceEventHandler(event ->
                  {
                      if (event.getState().equals("WORKFLOW_INSTANCE_COMPLETED"))
                      {
                          latch.countDown();
                      }
                  })
                  .open();

            start = System.currentTimeMillis();
            latch.await();
            end = System.currentTimeMillis();

            System.out.println("Took " + (end - start) + "ms to complete " + instances + " workflow instances");
        }

    }

}
