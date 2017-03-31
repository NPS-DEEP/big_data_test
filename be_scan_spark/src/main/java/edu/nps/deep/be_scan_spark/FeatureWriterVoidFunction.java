package edu.nps.deep.be_scan_spark;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.Writer;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class FeatureWriterVoidFunction implements Serializable,
      VoidFunction<Tuple2<Long, String>> {

  private BufferedWriter out;

  public FeatureWriterVoidFunction() {
    String timestamp = new SimpleDateFormat(
                          "yyyy-MM-dd hh-mm-ss'.tsv'").format(new Date());
    String filename = "feature_capture_" + timestamp;

    if (new File(filename).exists()) {
      throw new RuntimeException("ERROR: file " + filename +
                                 " exists.  Aborting.");
    }

    // open filename for appending
    try {
      FileWriter fw = new FileWriter(filename, true); // append
      out = new BufferedWriter(fw);
    } catch (IOException e) {
      // bad
      System.err.println("Error saving to file " + filename);
      throw new RuntimeException(e);
    }
  }

  public void call(Tuple2<Long, String> tuple) {

    // write feature string to file
    try {
      out.write(tuple._2());
      out.write("\n");
    } catch (IOException e) {
      // bad
      System.err.println("Error saving feature to file");
      throw new RuntimeException(e);
    }
  }

/*
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
*/

}

