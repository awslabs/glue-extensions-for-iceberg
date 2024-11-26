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

/**
 * Adapter classes for allowing V1 credential providers to be used with SDKV2.
 * This is the only package where use of aws v1 classes are permitted;
 * all instantiations of objects here must use reflection to probe for
 * availability or be prepared to catch exceptions which may be raised
 * if the v1 SDK isn't found on the classpath
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
package software.amazon.glue.s3a.adapter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;