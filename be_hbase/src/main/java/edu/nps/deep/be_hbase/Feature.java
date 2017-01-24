package edu.nps.deep.be_hbase;

/**
 * One extracted feature, consisting of a forensic path, feature bytes,
   and context bytes.
 */
public final class Feature {

  public final String filename;
  public final String path;
  public final String feature;
  public final String context;

  public Feature(String filename, String path, String feature, String context) {
    this.path = path;
    this.filename = filename;
    this.feature = feature;
    this.context = context;
  }

  public String toString() {
    return "'" + filename + "', '" + path + "', '" +
                 feature + "', '" + context + "'";
  }
}

