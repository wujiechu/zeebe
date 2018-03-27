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
package io.zeebe.broker.task.processor;

import static org.junit.Assert.fail;

import org.junit.Test;

public class YeOldeLockTaskStreamProcessorTest
{
    @Test
    public void foo()
    {
        fail("understand and port remaining tests");
    }

//    private static final byte[] TASK_TYPE = getBytes("test-task");
//    private static final DirectBuffer TASK_TYPE_BUFFER = new UnsafeBuffer(TASK_TYPE);
//
//    private static final byte[] ANOTHER_TASK_TYPE = getBytes("another-task");
//    private static final DirectBuffer ANOTHER_TASK_TYPE_BUFFER = new UnsafeBuffer(ANOTHER_TASK_TYPE);
//
//    private TaskSubscription subscription;
//    private TaskSubscription anotherSubscription;
//
//    private LockTaskStreamProcessor streamProcessor;
//
//    @Mock
//    private LoggedEvent mockLoggedEvent;
//
//    @Mock
//    private LogStream mockLogStream;
//
//    @Rule
//    public ExpectedException thrown = ExpectedException.none();
//
//    @Rule
//    public MockStreamProcessorController<TaskEvent> mockController = new MockStreamProcessorController<>(TaskEvent.class, event -> event.setRetries(3), TASK_EVENT, 1L);
//
//    @Before
//    public void setup() throws InterruptedException, ExecutionException
//    {
//        MockitoAnnotations.initMocks(this);
//
//        // fix the current time to calculate lock time
//        ClockUtil.setCurrentTime(Instant.now());
//
//        streamProcessor = new LockTaskStreamProcessor(TASK_TYPE_BUFFER);
//
//        subscription = new TaskSubscription(0, TASK_TYPE_BUFFER, Duration.ofMinutes(5).toMillis(), wrapString("owner-1"), 11);
//        subscription.setSubscriberKey(1L);
//        subscription.setCredits(3);
//
//        anotherSubscription = new TaskSubscription(0, TASK_TYPE_BUFFER, Duration.ofMinutes(10).toMillis(), wrapString("owner-2"), 12);
//        anotherSubscription.setSubscriberKey(2L);
//        anotherSubscription.setCredits(2);
//
//        final StreamProcessorContext context = new StreamProcessorContext();
//        context.setLogStream(mockLogStream);
//
//        mockController.initStreamProcessor(streamProcessor, context);
//    }
//
//    @After
//    public void cleanUp()
//    {
//        ClockUtil.reset();
//    }
//
//    @Test
//    public void shouldAcceptEventForReprocessingWithSubscribedType()
//    {
//        final LoggedEvent loggedEvent = mockController.buildLoggedEvent(2L, event -> event
//                .setState(TaskState.CREATED)
//                .setType(TASK_TYPE_BUFFER));
//
//        final EventFilter eventFilter = LockTaskStreamProcessor.reprocessingEventFilter(TASK_TYPE_BUFFER);
//
//        assertThat(eventFilter.applies(loggedEvent)).isTrue();
//    }
//
//    @Test
//    public void shouldRejectEventForReprocessingWithDifferentType()
//    {
//        final LoggedEvent loggedEvent = mockController.buildLoggedEvent(2L, event -> event
//                .setState(TaskState.CREATED)
//                .setType(ANOTHER_TASK_TYPE_BUFFER));
//
//        final EventFilter eventFilter = LockTaskStreamProcessor.reprocessingEventFilter(TASK_TYPE_BUFFER);
//
//        assertThat(eventFilter.applies(loggedEvent)).isFalse();
//    }
//
//    protected long lockTimeOf(TaskSubscription subscription)
//    {
//        return ClockUtil.getCurrentTime().plusMillis(subscription.getLockDuration()).toEpochMilli();
//    }

}
