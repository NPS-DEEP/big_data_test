package edu.nps.deep.be_cluster;

/**
 * One extracted feature, consisting of a forensic path and the bytes
 * of the feature.
 */
public final class ExtractedFeature {

  public final String forensicPath;
  public final String featureBytes;

  public ExtractedFeature(String path, String bytes) {
    forensicPath = path;
    featureBytes = bytes;
  }

  public String toString() {
    return forensicPath + ", '" + featureBytes + "'";
  }
}

