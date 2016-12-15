package edu.nps.deep.be_cluster;

import java.util.Vector;
import java.util.Iterator;
import java.io.Serializable;

/**
 * Wrap Template with this concrete class to avoid inferred type mismatch.
 */
public final class Features implements Serializable {
  private final Vector<Feature> features = new Vector<Feature>();

  public int size() {
    return features.size();
  }

  public void add(Feature feature) {
    features.add(feature);
  }

  public Iterator<Feature> iterator() {
    return features.iterator();
  }

  public String toString() {
    Iterator it = iterator();
    String s = new String();
    while (it.hasNext()) {
      s += "(" + it.next() + ")";
    }
    return s;
  }
}

