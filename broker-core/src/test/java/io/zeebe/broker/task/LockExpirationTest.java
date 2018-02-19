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
package io.zeebe.broker.task;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.test.broker.protocol.clientapi.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class LockExpirationTest
{
    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientApiRule apiRule = new ClientApiRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

    @Test
    public void shouldExpireLockedTask()
    {
        // given
        final String taskType = "foo";
        final long taskKey1 = createTask(taskType);

        final long lockTime = 1000L;

        apiRule.openTaskSubscription(
            apiRule.getDefaultPartitionId(),
            taskType,
            lockTime);

        waitUntil(() -> apiRule.numSubscribedEventsAvailable() == 1);
        apiRule.moveMessageStreamToTail();

        // when expired
        doRepeatedly(() ->
        {
            brokerRule.getClock().addTime(TaskQueueManagerService.LOCK_EXPIRATION_INTERVAL);
        }).until(v -> apiRule.numSubscribedEventsAvailable() == 1);


        // then locked again
        final List<SubscribedEvent> events = apiRule.topic()
                                                     .receiveEvents(TestTopicClient.taskEvents())
                                                     .limit(8)
                                                     .collect(Collectors.toList());

        assertThat(events).extracting(e -> e.key()).contains(taskKey1);
        assertThat(events).extracting(e -> e.event().get("state"))
                          .containsExactly("CREATE", "CREATED", "LOCK", "LOCKED", "EXPIRE_LOCK", "LOCK_EXPIRED", "LOCK", "LOCKED");
    }

    @Test
    public void shouldExpireMultipleLockedTasksAtOnce()
    {
        // given
        final String taskType = "foo";
        final long taskKey1 = createTask(taskType);
        final long taskKey2 = createTask(taskType);

        final long lockTime = 1000L;

        apiRule.openTaskSubscription(
                apiRule.getDefaultPartitionId(),
                taskType,
                lockTime);

        waitUntil(() -> apiRule.numSubscribedEventsAvailable() == 2); // both tasks locked
        apiRule.moveMessageStreamToTail();

        // when
        doRepeatedly(() ->
        {
            brokerRule.getClock().addTime(TaskQueueManagerService.LOCK_EXPIRATION_INTERVAL);
        }).until(v -> apiRule.numSubscribedEventsAvailable() == 2);

        // then
        final List<SubscribedEvent> expiredEvents = apiRule.topic()
                                                    .receiveEvents(TestTopicClient.taskEvents())
                                                    .limit(16)
                                                    .collect(Collectors.toList());

        assertThat(expiredEvents)
            .filteredOn(e -> e.event().get("state").equals("LOCKED"))
            .hasSize(4)
            .extracting(e -> e.key()).containsExactly(taskKey1, taskKey2, taskKey1, taskKey2);

        assertThat(expiredEvents)
            .filteredOn(e -> e.event().get("state").equals("LOCK_EXPIRED"))
            .extracting(e -> e.key()).containsExactlyInAnyOrder(taskKey1, taskKey2);
    }

    protected long createTask(String type)
    {
        final ExecuteCommandResponse response = apiRule.createCmdRequest()
            .eventTypeTask()
            .command()
                .put("state", "CREATE")
                .put("type", type)
                .put("retries", 3)
                .done()
            .sendAndAwait();

        return response.key();
    }

}
