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

package software.amazon.glue.s3a.impl.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.store.LogExactlyOnce;

/**
 * Factory for creating controllers.
 * <p>
 * It currently only supports Log4J as a back end.
 */
public final class LogControllerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(LogControllerFactory.class);

  /**
   * Log once: when there are logging issues, logging lots just
   * makes it worse.
   */
  private static final LogExactlyOnce LOG_ONCE = new LogExactlyOnce(LOG);

  /**
   * Class name of log controller implementation to be loaded
   * through reflection.
   * {@value}.
   */
  private static final String LOG4J_CONTROLLER =
      "org.apache.hadoop.fs.s3a.impl.logging.Log4JController";

  private LogControllerFactory() {
  }

  /**
   * Create a controller. Failure to load is logged at debug
   * and null is returned.
   * @param classname name of controller to load and create.
   * @return the instantiated controller or null if it failed to load
   */
  public static LogControl createController(String classname) {
    try {
      Class<?> clazz = Class.forName(classname);
      return (LogControl) clazz.newInstance();
    } catch (Exception e) {
      LOG_ONCE.debug("Failed to create controller {}: {}", classname, e, e);
      return null;
    }
  }

  /**
   * Create a Log4J controller.
   * @return the instantiated controller or null if the class can't be instantiated.
   */
  public static LogControl createLog4JController() {
    return createController(LOG4J_CONTROLLER);
  }

  /**
   * Create a controller, Log4j or falling back to a stub implementation.
   * @return the instantiated controller or empty() if the class can't be instantiated.
   */
  public static LogControl createController() {
    final LogControl controller = createLog4JController();
    return controller != null
        ? controller
        : new StubLogControl();
  }

  /**
   * Stub controller which always reports false.
   */
  private static final class StubLogControl extends LogControl {

    @Override
    protected boolean setLevel(final String log, final LogLevel level) {
      return false;

    }
  }
}
