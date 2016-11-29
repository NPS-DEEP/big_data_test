package edu.nps.deep.be_cluster;

/**
 * One extracted feature, consisting of a forensic path and the bytes
 * of the feature.
 */
public final class Feature {

  public final String forensicPath;
  public final String featureBytes;

  public Feature(String path, String bytes) {
    forensicPath = path;
    featureBytes = bytes;
  }

  public String toString() {
    return forensicPath + ", '" + featureBytes + "'";
  }
}

