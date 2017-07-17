// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.be_scan_spark_avro;

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
public final class BEScanAvroReader
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
    scanner = new edu.nps.deep.be_scan.Scanner(scanEngine, "unused output filename");
  }

  private String filename;
  private boolean isParsed = false;
  private BufferRecordReader reader;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
                                throws IOException, InterruptedException {

    // open the reader
    reader = new BufferRecordReader(split, context);

    // get the filename string for reporting artifacts
    filename = ((FileSplit)split).getPath().toString();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // call exactly once
    if (isParsed) {
      throw new IOException("Error: BESanAvroReader next already called");
    }

    // parse all records in the split
long biggestReadTime = 0;
long biggestReadAndScanTime = 0;
long biggestReadAndScanTimeOffset = 0;

    while(reader.hasNext()) {
long startTime = System.nanoTime();
      BufferRecordReader.BufferRecord record = reader.next();
long deltaReadTime = System.nanoTime() - startTime;

      // scan the buffer
      String success = scanner.scan(
                          filename,
                          java.math.BigInteger.valueOf(record.offset),
                          "", record.buffer, record.buffer.length);
long deltaReadAndScanTime = System.nanoTime() - startTime;
if (deltaReadAndScanTime > biggestReadAndScanTime) {
  biggestReadTime = deltaReadTime;
  biggestReadAndScanTime = deltaReadAndScanTime;
  biggestReadAndScanTimeOffset = record.offset;
}
      if (!success.equals("")) {
        throw new IOException("Error in scan: '" + success + "'");
      }
    }

// show the section that took the longest
System.out.println("Read timing: " + biggestReadAndScanTime + " " + biggestReadTime + " " + filename + " " + record.offset);

    // done
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

