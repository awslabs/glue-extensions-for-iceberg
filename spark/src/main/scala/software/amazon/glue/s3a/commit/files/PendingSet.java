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

package software.amazon.glue.s3a.commit.files;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import software.amazon.glue.s3a.commit.ValidationFailure;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.JsonSerialization;

import static software.amazon.glue.s3a.commit.CommitUtils.validateCollectionClass;
import static software.amazon.glue.s3a.commit.ValidationFailure.verify;

/**
 * Persistent format for multiple pending commits.
 * Contains 0 or more {@link SinglePendingCommit} entries; validation logic
 * checks those values on load.
 * <p>
 * The statistics published through the {@link IOStatisticsSource}
 * interface are the static ones marshalled with the commit data;
 * they may be empty.
 * </p>
 * <p>
 * As single commits are added via {@link #add(SinglePendingCommit)},
 * any statistics from those commits are merged into the aggregate
 * statistics, <i>and those of the single commit cleared.</i>
 * </p>
 */
@SuppressWarnings("unused")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PendingSet extends PersistentCommitData<PendingSet> {
  private static final Logger LOG = LoggerFactory.getLogger(PendingSet.class);

  /**
   * Supported version value: {@value}.
   * If this is changed the value of {@link #serialVersionUID} will change,
   * to avoid deserialization problems.
   */
  public static final int VERSION = 3;

  /**
   * Serialization ID: {@value}.
   */
  private static final long serialVersionUID = 0x11000 + VERSION;


  /** Version marker. */
  private int version = VERSION;

  /** Job ID, if known. */
  private String jobId = "";

  /**
   * Commit list.
   */
  private List<SinglePendingCommit> commits;

  /**
   * Any custom extra data committer subclasses may choose to add.
   */
  private final Map<String, String> extraData = new HashMap<>(0);

  /**
   * IOStatistics.
   */
  @JsonProperty("iostatistics")
  private IOStatisticsSnapshot iostats = new IOStatisticsSnapshot();

  public PendingSet() {
    this(0);
  }


  public PendingSet(int size) {
    commits = new ArrayList<>(size);
  }

  /**
   * Get a shared JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<PendingSet> serializer() {
    return new JsonSerialization<>(PendingSet.class, false, false);
  }


  /**
   * Load an instance from a file, then validate it.
   * @param fs filesystem
   * @param path path
   * @param status status of file to load
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws ValidationFailure if the data is invalid
   */
  public static PendingSet load(FileSystem fs, Path path,
      @Nullable FileStatus status)
      throws IOException {
    LOG.debug("Reading pending commits in file {}", path);
    PendingSet instance = serializer().load(fs, path, status);
    instance.validate();
    return instance;
  }

  /**
   * Add a commit.
   * @param commit the single commit
   */
  public void add(SinglePendingCommit commit) {
    commits.add(commit);
    // add any statistics.
    IOStatisticsSnapshot st = commit.getIOStatistics();
    if (st != null) {
      iostats.aggregate(st);
      st.clear();
    }
  }

  /**
   * Deserialize via java Serialization API: deserialize the instance
   * and then call {@link #validate()} to verify that the deserialized
   * data is valid.
   * @param inStream input stream
   * @throws IOException IO problem or validation failure
   * @throws ClassNotFoundException reflection problems
   */
  private void readObject(ObjectInputStream inStream) throws IOException,
      ClassNotFoundException {
    inStream.defaultReadObject();
    validate();
  }

  /**
   * Validate the data: those fields which must be non empty, must be set.
   * @throws ValidationFailure if the data is invalid
   */
  public void validate() throws ValidationFailure {
    verify(version == VERSION, "Wrong version: %s", version);
    validateCollectionClass(extraData.keySet(), String.class);
    validateCollectionClass(extraData.values(), String.class);
    Set<String> destinations = new HashSet<>(commits.size());
    validateCollectionClass(commits, SinglePendingCommit.class);
    for (SinglePendingCommit c : commits) {
      c.validate();
      verify(!destinations.contains(c.getDestinationKey()),
          "Destination %s is written to by more than one pending commit",
          c.getDestinationKey());
      destinations.add(c.getDestinationKey());
    }
  }

  @Override
  public byte[] toBytes(JsonSerialization<PendingSet> serializer) throws IOException {
    return serializer.toBytes(this);
  }

  /**
   * Number of commits.
   * @return the number of commits in this structure.
   */
  public int size() {
    return commits != null ? commits.size() : 0;
  }

  @Override
  public IOStatistics save(final FileSystem fs,
      final Path path,
      final JsonSerialization<PendingSet> serializer) throws IOException {
    return saveFile(fs, path, this, serializer, true);
  }

  /** @return the version marker. */
  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  /**
   * @return commit list.
   */
  public List<SinglePendingCommit> getCommits() {
    return commits;
  }

  public void setCommits(List<SinglePendingCommit> commits) {
    this.commits = commits;
  }

  /**
   * Set/Update an extra data entry.
   * @param key key
   * @param value value
   */
  public void putExtraData(String key, String value) {
    extraData.put(key, value);
  }

  /** @return Job ID, if known. */
  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  @Override
  public IOStatisticsSnapshot getIOStatistics() {
    return iostats;
  }

  public void setIOStatistics(final IOStatisticsSnapshot ioStatistics) {
    this.iostats = ioStatistics;
  }
}
