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

package software.amazon.glue.s3a.statistics;

/**
 * Interface for PUT tracking.
 * It is subclassed by {@link BlockOutputStreamStatistics},
 * so that operations performed by the PutTracker update
 * the stream statistics.
 * Having a separate interface helps isolate operations.
 */
public interface PutTrackerStatistics extends S3AStatisticInterface {
}
