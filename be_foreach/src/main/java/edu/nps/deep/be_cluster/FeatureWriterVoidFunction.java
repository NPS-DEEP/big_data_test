package edu.nps.deep.be_cluster;

import java.util.Iterator;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.Writer;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.util.*;

public class FeatureWriterVoidFunction implements Serializable,
      VoidFunction<Tuple2<Long, Features>> {

  public final String filename;

  public FeatureWriterVoidFunction(String filename) {
    this.filename = filename;
    if (new File(filename).exists()) {
      throw new RuntimeException("ERROR: file " + filename +
                                 " exists.  Aborting.");
    }
  }

  public void call(Tuple2<Long, Features> tuple) {

    // open filename for appending
    try {
      FileWriter fw = new FileWriter(filename, true); // append
      BufferedWriter out = new BufferedWriter(fw);
      writeFeatures(tuple._2(), out);
      out.close();
      fw.close();
    } catch (IOException e) {
      // bad
      System.err.println("Error saving to file " + filename);
      throw new RuntimeException(e);
    }
  }

  private String escape(String input) {
    StringBuilder output = new StringBuilder();
    for (int i=0; i<input.length(); ++i) {
      char ch = input.charAt(i);
      if (ch < ' ' || ch > '~' || ch == '\\') {
        // show as \xXX
        output.append(String.format("\\x%02X", (int)ch));
      } else {
        // show ascii character
        output.append(ch);
      }
    }
    return output.toString();
  }

  public void writeFeatures(Features features, Writer out) throws IOException {

    // do not write key

    // write each feature
    Iterator<Feature> it = features.iterator();
    while (it.hasNext()) {
      Feature feature = it.next();
      out.write(feature.path + "\t" +
                escape(feature.feature) + "\t" +
                escape(feature.context) + "\n");
    }
  }

/*
  public synchronized 
  void close(TaskAttemptContext context) throws IOException {
    out.close();
  }
*/
}

