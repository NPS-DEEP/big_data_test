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
                         Long, Long> {

  static {
    String rootDirectory = SparkFiles.getRootDirectory();
    String unused = SparkFiles.get("libbe_scan_jni.so");
System.out.println("zzzzzzzzzzzzzzzzzzzzzz unused: " + unused);
    java.io.File path = new java.io.File(rootDirectory, "libbe_scan_jni.so");
    System.load(path.getAbsolutePath());
  }

  private boolean isParsed = false;
  private SplitReader splitReader;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
                                throws IOException, InterruptedException {

    // open the SplitReader
    splitReader = new SplitReader(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // maybe parse the split into features
    if (!isParsed) {

      // open the scanner
      edu.nps.deep.be_scan.BEScan scanner =
                       new edu.nps.deep.be_scan.BEScan("zzzz setting");

      // scan into the DB
      scanner.scan(splitReader.filename,
                   splitReader.splitStart,
                   "",       // recursion path
                   splitReader.buffer,
                   splitReader.buffer.length);

      // done parsing, the scan is really what we need, not an RDD.
      isParsed = true;
    }

    // never return key value, parsing is what was needed.
    return false;
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public Long getCurrentValue() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public float getProgress() throws IOException {
    return (isParsed) ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // no action
  }
}

