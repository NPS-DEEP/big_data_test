package edu.nps.deep.be_hbase;

/**
 * One extracted feature, consisting of filename, forensic path, and feature.
 */
public final class Feature {

  public final String filename;
  public final String path;
  public final String feature;

  public Feature(String filename, String path, String feature) {
    this.filename = filename;
    this.path = path;
    this.feature = feature;
  }

  public String toString() {
    return "'" + filename + "', '" + path + "', '" + feature + "'";
  }
}

