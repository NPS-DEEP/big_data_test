// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.be_scan_spark2;

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

/**
 * Scans and imports all artifacts at the first call to nextKeyValue().
 * Nothing meaningful is returned for the RDD, and returned constants
 * should be changed to null.
 */
public final class BEScanSplitReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         Long, String> {

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
  private boolean isParsed = false;
  private BufferReader reader;

  // consume artifacts, change this to DB as desired
  void consumeArtifacts() {
    while (!scanner.empty()) {
      edu.nps.deep.be_scan.Artifact artifact = scanner.get();
      // for now, just print it
      System.out.println(artifact.toString());
    }
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
                                throws IOException, InterruptedException {

    // open the reader
    reader = new BufferReader(split, context);

    // get the filename string for reporting artifacts
    filename = ((FileSplit)split).getPath().toString();

    // set up to read this split, no recursion prefix
    scanner.scanSetup(filename, "");
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // previous_buffer
    byte[] previous_buffer = null;

    // call exactly once
    if (isParsed) {
      throw new IOException("Error: BESanAvroReader next already called");
    }

    // timing
    long biggestDelta = 0;
    long tRead = 0;
    long tScan = 0;
    long tConsume = 0;

    // parse all records in the split
    while(true) {
      // timing
      long t0 = System.nanoTime();

      BufferReader.BufferRecord record = reader.next();

      // timing
      long t1 = System.nanoTime();

      // scan the buffer
      String success = scanner.scan(record.offset,
                                    previous_buffer, record.buffer);
      if (!success.equals("")) {
        throw new IOException("Error: " + success);
      }

      // timing
      long t2 = System.nanoTime();

      consumeArtifacts();

      // timing
      long t3 = System.nanoTime();
      if (t3 - t0 > biggestDelta) {
        tRead = t1 - t0;
        tScan = t2 - t1;
        tConsume = t3 - t2;
        biggestDelta = t3 - t0;
      }

      if (reader.hasNext()) {
        previous_buffer = record.buffer;
        continue;

      } else {
        success = scanner.scanFinalize(record.offset,
                                              previous_buffer, record.buffer);
        if (!success.equals("")) {
          throw new IOException("Error: " + success);
        }

        consumeArtifacts();
        break;
      }
    }
    // done
    isParsed = true;

    // timing
    System.out.println("Read timing total: " + biggestDelta + " read: " + tRead + " scan: " + tScan + " consume: " + tConsume + " " + filename + " " + record.offset);
    return false;
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public String getCurrentValue() throws IOException, InterruptedException {
    return "done";
  }

  @Override
  public float getProgress() throws IOException {
    return isParsed ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // no action
  }
}

