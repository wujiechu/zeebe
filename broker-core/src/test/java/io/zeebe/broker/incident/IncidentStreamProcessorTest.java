package io.zeebe.broker.incident;

import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;

import io.zeebe.broker.task.processor.StreamProcessorRule;

public class IncidentStreamProcessorTest
{
    @Rule
    public StreamProcessorRule rule = new StreamProcessorRule();

    /**
     * Event order:
     *
     * Task FAILED -> UPDATE_RETRIES -> RETRIES UPDATED -> Incident CREATE -> Incident CREATE_REJECTED
     */
    @Test
    public void shouldNotCreateIncidentIfRetriesAreUpdatedIntermittently()
    {
        fail("Implement");
    }

    @Test
    public void shouldNotResolveIncidentIfActivityTerminated()
    {
        fail("implement");
//      // given
//      // an incident for a failed workflow instance
//      final long failureEventPosition = writeWorkflowInstanceEvent(2L, wf -> wf
//              .setState(WorkflowInstanceState.ACTIVITY_READY)
//              .setWorkflowInstanceKey(1L));
//
//      writeIncidentEvent(3L, incident -> incident
//             .setState(IncidentState.CREATE)
//             .setWorkflowInstanceKey(1L)
//             .setActivityInstanceKey(2L)
//             .setFailureEventPosition(failureEventPosition));
//
//      agentRunnerService.waitUntilDone();
//
//      // when
//      // the payload of the workflow instance is updated (=> resolve incident)
//      writeWorkflowInstanceEvent(2L, wf -> wf
//              .setState(WorkflowInstanceState.PAYLOAD_UPDATED)
//              .setWorkflowInstanceKey(1L));
//
//      // and the workflow instance / activity is terminated (=> delete incident)
//      writeWorkflowInstanceEvent(2L, wf -> wf
//                                 .setState(WorkflowInstanceState.ACTIVITY_TERMINATED)
//                                 .setWorkflowInstanceKey(1L));
//
//      agentRunnerService.waitUntilDone();
//
//      // then don't resolve the incident
//      assertThat(getIncidentEvents())
//              .hasSize(6)
//              .extracting("state")
//              .containsExactly(IncidentState.CREATE,
//                               IncidentState.CREATED,
//                               IncidentState.RESOLVE,
//                               IncidentState.DELETE,
//                               IncidentState.RESOLVE_REJECTED,
//                               IncidentState.DELETED);
    }

}
