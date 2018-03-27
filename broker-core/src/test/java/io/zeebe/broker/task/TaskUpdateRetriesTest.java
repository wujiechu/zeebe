package io.zeebe.broker.task;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.ExecuteCommandResponse;
import io.zeebe.test.broker.protocol.clientapi.SubscribedEvent;

public class TaskUpdateRetriesTest
{
    private static final String TASK_TYPE = "foo";
    private static final int NEW_RETRIES = 20;

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
    public ClientApiRule apiRule = new ClientApiRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

    @Test
    public void shouldUpdateRetries()
    {
        // given
        createTask(TASK_TYPE);

        apiRule.openTaskSubscription(TASK_TYPE).await();

        final SubscribedEvent subscribedEvent = receiveSingleSubscribedEvent();

        Map<String, Object> event = subscribedEvent.event();
        event.put("retries", 0);
        final ExecuteCommandResponse failResponse = failTask(subscribedEvent.key(), event);

        event = failResponse.getEvent();
        event.put("retries", NEW_RETRIES);

        // when
        final ExecuteCommandResponse response = updateRetries(subscribedEvent.key(), event);

        // then
        final Map<String, Object> expectedEvent = new HashMap<>(event);
        expectedEvent.put("state", "RETRIES_UPDATED");

        assertThat(response.getEvent()).containsAllEntriesOf(expectedEvent);

        // and the task is published again
        final SubscribedEvent republishedEvent = receiveSingleSubscribedEvent();
        assertThat(republishedEvent.key()).isEqualTo(subscribedEvent.key());
        assertThat(republishedEvent.position()).isNotEqualTo(subscribedEvent.position());

        // and the task lifecycle is correct
        apiRule.openTopicSubscription("foo", 0).await();

        final int expectedTopicEvents = 10;

        final List<SubscribedEvent> taskEvents = doRepeatedly(() -> apiRule
                .moveMessageStreamToHead()
                .subscribedEvents()
                .filter(e -> e.subscriptionType() == SubscriptionType.TOPIC_SUBSCRIPTION &&
                      e.eventType() == EventType.TASK_EVENT)
                .limit(expectedTopicEvents)
                .collect(Collectors.toList()))
            .until(e -> e.size() == expectedTopicEvents);

        assertThat(taskEvents).extracting(e -> e.event().get("state"))
            .containsExactly(
                    "CREATE",
                    "CREATED",
                    "LOCK",
                    "LOCKED",
                    "FAIL",
                    "FAILED",
                    "UPDATE_RETRIES",
                    "RETRIES_UPDATED",
                    "LOCK",
                    "LOCKED");
    }

    @Test
    public void shouldRejectUpdateRetriesIfTaskNotFound()
    {
        // given
        final Map<String, Object> event = new HashMap<>();

        event.put("retries", NEW_RETRIES);
        event.put("type", TASK_TYPE);

        // when
        final ExecuteCommandResponse response = updateRetries(123, event);

        // then
        assertThat(response.getEvent()).containsEntry("state", "UPDATE_RETRIES_REJECTED");
    }

    @Test
    public void shouldRejectUpdateRetriesIfTaskCompleted()
    {
        // given
        createTask(TASK_TYPE);

        apiRule.openTaskSubscription(TASK_TYPE).await();

        final SubscribedEvent subscribedEvent = receiveSingleSubscribedEvent();

        Map<String, Object> event = subscribedEvent.event();
        final ExecuteCommandResponse completeResponse = completeTask(subscribedEvent.key(), event);

        event = completeResponse.getEvent();
        event.put("retries", NEW_RETRIES);

        // when
        final ExecuteCommandResponse response = updateRetries(subscribedEvent.key(), event);

        // then
        assertThat(response.getEvent()).containsEntry("state", "UPDATE_RETRIES_REJECTED");
    }

    @Test
    public void shouldRejectUpdateRetriesIfTaskLocked()
    {
        // given
        createTask(TASK_TYPE);

        apiRule.openTaskSubscription(TASK_TYPE).await();

        final SubscribedEvent subscribedEvent = receiveSingleSubscribedEvent();

        final Map<String, Object> event = subscribedEvent.event();
        event.put("retries", NEW_RETRIES);

        // when
        final ExecuteCommandResponse response = updateRetries(subscribedEvent.key(), event);

        // then
        assertThat(response.getEvent()).containsEntry("state", "UPDATE_RETRIES_REJECTED");
    }


    @Test
    public void shouldRejectUpdateRetriesIfRetriesZero()
    {
        // given
        createTask(TASK_TYPE);

        apiRule.openTaskSubscription(TASK_TYPE).await();

        final SubscribedEvent subscribedEvent = receiveSingleSubscribedEvent();

        Map<String, Object> event = subscribedEvent.event();
        event.put("retries", 0);
        final ExecuteCommandResponse failResponse = failTask(subscribedEvent.key(), event);

        event = failResponse.getEvent();
        event.put("retries", 0);

        // when
        final ExecuteCommandResponse response = updateRetries(subscribedEvent.key(), event);

        // then
        assertThat(response.getEvent()).containsEntry("state", "UPDATE_RETRIES_REJECTED");
    }

    @Test
    public void shouldRejectUpdateRetriesIfRetriesLessThanZero()
    {
        // given
        createTask(TASK_TYPE);

        apiRule.openTaskSubscription(TASK_TYPE).await();

        final SubscribedEvent subscribedEvent = receiveSingleSubscribedEvent();

        Map<String, Object> event = subscribedEvent.event();
        event.put("retries", 0);
        final ExecuteCommandResponse failResponse = failTask(subscribedEvent.key(), event);

        event = failResponse.getEvent();
        event.put("retries", -1);

        // when
        final ExecuteCommandResponse response = updateRetries(subscribedEvent.key(), event);

        // then
        assertThat(response.getEvent()).containsEntry("state", "UPDATE_RETRIES_REJECTED");
    }

    private ExecuteCommandResponse createTask(String type)
    {
        return apiRule.createCmdRequest()
            .eventTypeTask()
            .command()
                .put("state", "CREATE")
                .put("type", type)
                .put("retries", 1)
            .done()
            .sendAndAwait();
    }

    private ExecuteCommandResponse failTask(long key, Map<String, Object> event)
    {
        return apiRule.createCmdRequest()
            .eventTypeTask()
            .key(key)
            .command()
                .putAll(event)
                .put("state", "FAIL")
            .done()
            .sendAndAwait();
    }

    private ExecuteCommandResponse updateRetries(long key, Map<String, Object> event)
    {
        return apiRule.createCmdRequest()
            .eventTypeTask()
            .key(key)
            .command()
                .putAll(event)
                .put("state", "UPDATE_RETRIES")
            .done()
            .sendAndAwait();
    }

    private ExecuteCommandResponse completeTask(long key, Map<String, Object> event)
    {
        return apiRule.createCmdRequest()
            .eventTypeTask()
            .key(key)
            .command()
                .putAll(event)
                .put("state", "COMPLETE")
            .done()
            .sendAndAwait();
    }

    private SubscribedEvent receiveSingleSubscribedEvent()
    {
        waitUntil(() -> apiRule.numSubscribedEventsAvailable() == 1);
        return apiRule.subscribedEvents().findFirst().get();
    }
}
