package edu.nps.deep.be_cluster;

import java.util.ArrayDeque;
import java.io.Serializable;

/**
 * Wrap Template with this concrete class to avoid inferred type mismatch.
 */
public final class Features implements Serializable {
  private final ArrayDeque<Feature> features = new ArrayDeque<Feature>();

  public int size() {
    return features.size();
  }

  public void add(Feature feature) {
    features.add(feature);
  }

  public Feature remove() {
    return features.remove();
  }
}

