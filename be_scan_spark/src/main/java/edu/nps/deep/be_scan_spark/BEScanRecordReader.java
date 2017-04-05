// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.be_scan_spark;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkFiles;
import scala.Tuple2;

// See be_scan/java_bindings/Tests.java for example usage of the be_scan API.
import edu.nps.deep.be_scan.BEScan;

/**
 * Scans and imports all artifacts at the first call to nextKeyValue().
 * Nothing is actually ever returned to go into an RDD.
 */
public final class BEScanRecordReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         Long, String> {

  static {
    System.load(SparkFiles.get("libbe_scan.so"));
    System.load(SparkFiles.get("libbe_scan_jni.so"));
  }

  private boolean isParsed = false;
  private SplitReader splitReader;
  private edu.nps.deep.be_scan.BEScan scanner;
  private String feature;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
                                throws IOException, InterruptedException {

    // open the SplitReader
    splitReader = new SplitReader(split, context);

    // open the scanner
    scanner = new edu.nps.deep.be_scan.BEScan("email",
                             splitReader.buffer, splitReader.buffer.length);
//    scanner = new edu.nps.deep.be_scan.BEScan("email", splitReader.buffer);

    // make sure the buffer was allocated
    if (!scanner.getIsInitialized()) {
      throw new IOException("memory allocation for scanner buffer failed");
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    edu.nps.deep.be_scan.Artifact artifact = scanner.nextArtifact();

    if (artifact.getArtifactClass().equals("")) {
      // no more artifacts
      feature = "";
      return false;
    } else {
      byte[] javaArtifact = artifact.javaArtifact();
      byte[] javaContext= artifact.javaContext();

      StringBuilder sb = new StringBuilder();

      sb.append(artifact.getArtifactClass());          // artifact class
      sb.append(" ");                                  // space
      sb.append(splitReader.filename);                 // filename
      sb.append(" ");                                  // space
      sb.append("");                                   // recursion path
      sb.append(String.valueOf(splitReader.splitStart +
                     artifact.getBufferOffset()));     // file offset
      sb.append("\t");                                 // tab
      sb.append(escape(new String(javaArtifact)));     // artifact
      sb.append("\t");                                 // tab
      sb.append(escape(new String(javaContext)));      // artifact
      
      feature = sb.toString();
      return true;
    }
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public String getCurrentValue() throws IOException, InterruptedException {

    // for now, this is the output
    System.out.println("bulk_extractor " + feature);

    return feature;
  }

  @Override
  public float getProgress() throws IOException {
    return feature.equals("") ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // no action
  }

  private String escape(String input) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<input.length(); ++i) {
      char c = input.charAt(i);
      if (c < ' ' || c > '~' || c == '\\') {
        // show as \xXX
        sb.append(String.format("\\x%02X", (int)c&0xff));
      } else {
        // show ascii character
        sb.append(c);
      }
    }
    return sb.toString();
  }
}

