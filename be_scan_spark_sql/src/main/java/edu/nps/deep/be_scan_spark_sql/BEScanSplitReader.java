// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.be_scan_spark_sql;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.NullWritable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkFiles;
import scala.Tuple2;

import edu.nps.deep.be_scan.Artifact;

/**
 * Scans and provides artifacts.
 */
public final class BEScanSplitReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         SerializableArtifact, NullWritable> {

  // static scan engine and scanner
  private static final edu.nps.deep.be_scan.ScanEngine scanEngine;
  private static final edu.nps.deep.be_scan.Scanner scanner;
  static {
    System.load(SparkFiles.get("libstdc++.so"));
    System.load(SparkFiles.get("libicudata.so"));
    System.load(SparkFiles.get("libicuuc.so"));
    System.load(SparkFiles.get("liblightgrep.so"));
    System.load(SparkFiles.get("liblightgrep_wrapper.so"));
    System.load(SparkFiles.get("libbe_scan.so"));
    System.load(SparkFiles.get("libbe_scan_jni.so"));

    scanEngine = new edu.nps.deep.be_scan.ScanEngine("email");
    scanner = new edu.nps.deep.be_scan.Scanner(scanEngine);
  }

  private String filename;
  private BufferReader reader;
  byte[] previous_buffer = null;
  private SerializableArtifact serializableArtifact = null;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
                                throws IOException, InterruptedException {

    // open the reader
    reader = new BufferReader(split, context);

    // get the filename string for reporting artifacts
    filename = ((FileSplit)split).getPath().toString();

    // set up to read this split, no recursion prefix
    scanner.scanSetup(filename, "");

    // done if no data
    if (!reader.hasNext()) {
      // done
      return;
    }

    // read the first buffer
    previous_buffer = null;
    BufferReader.BufferRecord record = reader.next();

    // scan the first buffer
    String success = scanner.scan(record.offset,
                                  previous_buffer, record.buffer);
    if (!success.equals("")) {
      throw new IOException("Error: " + success);
    }

    // move current buffer to previous buffer
    previous_buffer = record.buffer;
  }

  private void scanAsNeeded() throws IOException {

    // scan buffers until we get artifacts
    while (reader.hasNext() && scanner.empty()) {

      // read next
      BufferReader.BufferRecord record = reader.next();

      // scan next
      String success = scanner.scan(record.offset,
                                    previous_buffer, record.buffer);
      if (!success.equals("")) {
        throw new IOException("Error: " + success);
      }

      // move current buffer to previous buffer
      previous_buffer = record.buffer;
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // maybe scan more
    scanAsNeeded();
    if (scanner.empty()) {
      // no more artifacts
      serializableArtifact = null;
      return false;
    } else {
      // stage next artifact
      SerializableArtifact serializableArtifact = new
                               SerializableArtifact(scanner.get());
      return true;
    }
  }

  @Override
  public SerializableArtifact getCurrentKey()
                               throws IOException, InterruptedException {
    return serializableArtifact;
  }

  @Override
  public NullWritable getCurrentValue()
                              throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException {
    return reader.hasNext() ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // no action
  }
}

