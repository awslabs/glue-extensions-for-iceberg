/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.glue.s3a.commit.impl;

import org.apache.hadoop.fs.audit.AuditConstants;
import org.apache.hadoop.fs.audit.CommonAuditContext;
import software.amazon.glue.s3a.commit.CommitConstants;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import static org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext;

/**
 * Class to track/update context information to set
 * in threads.
 */
public final class AuditContextUpdater {

  /**
   * Job ID.
   */
  private final String jobId;

  /**
   * Task attempt ID for auditing.
   */
  private final String taskAttemptId;

  /**
   * Construct. Stores job information
   * to attach to thread contexts.
   * @param jobContext job/task context.
   */
  public AuditContextUpdater(final JobContext jobContext) {
    JobID contextJobID = jobContext.getJobID();
    this.jobId = contextJobID != null
            ? contextJobID.toString()
            : null;

    if (jobContext instanceof TaskAttemptContext) {
      // it's a task, extract info for auditing
      final TaskAttemptID tid = ((TaskAttemptContext) jobContext).getTaskAttemptID();
      this.taskAttemptId = tid != null
          ? tid.toString()
          : null;
    } else {
      this.taskAttemptId = null;
    }
  }

  public AuditContextUpdater(String jobId) {
    this.jobId = jobId;
    this.taskAttemptId = null;
  }

  /**
   * Add job/task info to current audit context.
   */
  public void updateCurrentAuditContext() {
    final CommonAuditContext auditCtx = currentAuditContext();
    if (jobId != null) {
      auditCtx.put(AuditConstants.PARAM_JOB_ID, jobId);
    } else {
      currentAuditContext().remove(AuditConstants.PARAM_JOB_ID);
    }
    if (taskAttemptId != null) {
      auditCtx.put(AuditConstants.PARAM_TASK_ATTEMPT_ID, taskAttemptId);
    } else {
      currentAuditContext().remove(CommitConstants.PARAM_TASK_ATTEMPT_ID);
    }

  }

  /**
   * Remove job/task info from the current audit context.
   */
  public void resetCurrentAuditContext() {
    currentAuditContext().remove(AuditConstants.PARAM_JOB_ID);
    currentAuditContext().remove(CommitConstants.PARAM_TASK_ATTEMPT_ID);
  }

}
