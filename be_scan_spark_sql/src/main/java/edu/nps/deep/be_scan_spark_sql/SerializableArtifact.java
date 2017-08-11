package edu.nps.deep.be_scan_spark_sql;

import java.io.Serializable;

import edu.nps.deep.be_scan.Artifact;

/**
 * Serializable JavaBean for storing artifacts in SQL.
 */
public final class SerializableArtifact implements Serializable {
  private String artifactClass;
  private String streamName;
  private String recursionPrefix;
  private long offset;
  private byte[] artifact;

  public SerializableArtifact(Artifact artifact) {
    artifactClass = artifact.getArtifactClass();
    streamName = artifact.getStreamName();
    recursionPrefix = artifact.getRecursionPrefix();
    offset = artifact.getOffset();
    this.artifact = artifact.javaArtifact();
  }

  public String getArtifactClass() { return artifactClass; }
  public void setArtifactClass(String ac) { artifactClass = ac; }
  public String getStreamName() { return streamName; }
  public void setStreamName(String sn) { streamName = sn; }
  public String getRecursionPrefix() { return recursionPrefix; }
  public void setRecursionPrefix(String rp) { recursionPrefix = rp; }
  public long getOffset() { return offset; }
  public void setOffset(long o) { offset = o; }
  public byte[] getArtifact() { return artifact; }
  public void setArtifact(byte[] a) { artifact = a; }

  public String toString() {
    return (getArtifactClass() + "," + getStreamName() + "," + getRecursionPrefix() + "," +
            getOffset() + new String(getArtifact()));
  }
}

