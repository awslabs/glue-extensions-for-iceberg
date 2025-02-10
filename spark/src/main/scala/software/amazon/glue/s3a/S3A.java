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

package software.amazon.glue.s3a;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import static software.amazon.glue.s3a.Constants.FS_S3A;

/**
 * S3A implementation of AbstractFileSystem.
 * This impl delegates to the S3AFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class S3A extends DelegateToFileSystem {

  public S3A(URI theUri, Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, new S3AFileSystem(), conf,
        theUri.getScheme().isEmpty() ? FS_S3A : theUri.getScheme(), false);
  }

  @Override
  public int getUriDefaultPort() {
    // return Constants.S3A_DEFAULT_PORT;
    return super.getUriDefaultPort();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("S3A{");
    sb.append("URI =").append(fsImpl.getUri());
    sb.append("; fsImpl=").append(fsImpl);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Close the file system; the FileContext API doesn't have an explicit close.
   */
  @Override
  protected void finalize() throws Throwable {
    fsImpl.close();
    super.finalize();
  }
}
