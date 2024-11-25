/*
 * Copyright amazon.com, Inc. Or Its AFFILIATES. all rights reserved.
 *
 * licensed under the apache License, version 2.0 (the "license").
 * you may not use this file except in Compliance WITH the license.
 * a copy of the license Is Located At
 *
 *  http://aws.amazon.Com/apache2.0
 *
 * or in the "license" file accompanying this file. this File Is distributed
 * on an "as is" basis, Without warranties or conditions of any kind, EITHER
 * express or Implied. see the license for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.glue.s3a.auth.delegation;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import software.amazon.glue.s3a.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtFetcher;
import org.apache.hadoop.security.token.Token;

/**
 * A DT fetcher for S3A.
 * This is a copy-and-paste of
 * {@code org.apache.hadoop.hdfs.HdfsDtFetcher}.
 *
 * It is only needed for the `hadoop dtutil` command.
 */
public class S3ADtFetcher implements DtFetcher {

  private static final String SERVICE_NAME = Constants.FS_S3A;

  private static final String FETCH_FAILED =
      "Filesystem not generating Delegation Tokens";

  /**
   * Returns the service name for HDFS, which is also a valid URL prefix.
   */
  public Text getServiceName() {
    return new Text(SERVICE_NAME);
  }

  public boolean isTokenRequired() {
    return UserGroupInformation.isSecurityEnabled();
  }

  /**
   *  Returns Token object via FileSystem, null if bad argument.
   *  @param conf - a Configuration object used with FileSystem.get()
   *  @param creds - a Credentials object to which token(s) will be added
   *  @param renewer  - the renewer to send with the token request
   *  @param url  - the URL to which the request is sent
   *  @return a Token, or null if fetch fails.
   */
  public Token<?> addDelegationTokens(Configuration conf,
      Credentials creds,
      String renewer,
      String url) throws Exception {
    if (!url.startsWith(getServiceName().toString())) {
      url = getServiceName().toString() + "://" + url;
    }
    FileSystem fs = FileSystem.get(URI.create(url), conf);
    Token<?> token = fs.getDelegationToken(renewer);
    if (token == null) {
      throw new DelegationTokenIOException(FETCH_FAILED + ": " + url);
    }
    creds.addToken(token.getService(), token);
    return token;
  }
}
