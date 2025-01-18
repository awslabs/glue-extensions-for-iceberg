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

package software.amazon.glue.s3a.select;

import static software.amazon.glue.s3a.select.SelectConstants.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience;
import software.amazon.glue.s3a.impl.InternalConstants;

/**
 * Constants for internal use in the software.amazon.glue.s3a module itself.
 * Please don't refer to these outside of this module &amp; its tests.
 * If you find you need to then either the code is doing something it
 * should not, or these constants need to be uprated to being
 * public and stable entries.
 */
@InterfaceAudience.Private
public final class InternalSelectConstants {

  private InternalSelectConstants() {
  }

  /**
   * An unmodifiable set listing the options
   * supported in {@code openFile()}.
   */
  public static final Set<String> SELECT_OPTIONS;

  /*
   * Build up the options, pulling in the standard set too.
   */
  static {
    // when adding to this, please keep in alphabetical order after the
    // common options and the SQL.
    HashSet<String> options = new HashSet<>(Arrays.asList(
        SELECT_SQL,
        SELECT_ERRORS_INCLUDE_SQL,
        SELECT_INPUT_COMPRESSION,
        SELECT_INPUT_FORMAT,
        SELECT_OUTPUT_FORMAT,
        CSV_INPUT_COMMENT_MARKER,
        CSV_INPUT_HEADER,
        CSV_INPUT_INPUT_FIELD_DELIMITER,
        CSV_INPUT_QUOTE_CHARACTER,
        CSV_INPUT_QUOTE_ESCAPE_CHARACTER,
        CSV_INPUT_RECORD_DELIMITER,
        CSV_OUTPUT_FIELD_DELIMITER,
        CSV_OUTPUT_QUOTE_CHARACTER,
        CSV_OUTPUT_QUOTE_ESCAPE_CHARACTER,
        CSV_OUTPUT_QUOTE_FIELDS,
        CSV_OUTPUT_RECORD_DELIMITER
    ));
    options.addAll(InternalConstants.STANDARD_OPENFILE_KEYS);
    SELECT_OPTIONS = Collections.unmodifiableSet(options);
  }
}
