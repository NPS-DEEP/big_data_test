// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.be_scan_spark_hbase;

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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkFiles;
import scala.Tuple2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import edu.nps.deep.be_scan.Artifact;

/**
 * Scans and provides artifacts.
 */
public final class BEScanSplitReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         Long, Put> {

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
  private Artifact artifact = null;

/*
  // consume artifacts, change this to DB as desired
  void consumeArtifacts() {
    while (!scanner.empty()) {
      edu.nps.deep.be_scan.Artifact artifact = scanner.get();
      // for now, just print it
      System.out.println(artifact.toString());
    }
  }
*/

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
      artifact = null;
      return false;
    } else {
      // stage next artifact
      artifact = scanner.get();
      return true;
    }
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public Put getCurrentValue() throws IOException, InterruptedException {
    // build key, value put object for HBase entry
    Put put = new Put(Bytes.toBytes("email," + filename + "," + 
                                    artifact.getArtifact()));
    put.addColumn(Bytes.toBytes("f"),        // column family
                  Bytes.toBytes("offset"),
                  Bytes.toBytes(String.valueOf(artifact.getOffset())));
    return put;
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

