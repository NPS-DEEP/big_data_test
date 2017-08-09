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

  private String filename = "not defined yet";
  private boolean isParsed = false;
  private BufferRecordReader reader;
private boolean badInitialization = false;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
//                                throws IOException, InterruptedException {

long start = -1;
try {

    // get the filename string for reporting artifacts
    filename = ((FileSplit)split).getPath().toString();

    // open the reader
    reader = new BufferRecordReader(split, context);
start = ((FileSplit)split).getStart();
//System.out.println("BEScanAvroReader.initialize zzzzzzzzzz '" + filename + "' start " + start);
//} catch (IOException e) {
} catch (Exception e) {
  System.out.println("BEScanAvroReader.initialize zzzzzzzzzz Exception failure in '" + filename + "' at " + start + ": " + e);
  badInitialization = true;
//} catch (IOException e) {
//  System.out.println("BEScanAvroReader.initialize IOException failure in '" + filename + "' at " + start);
//  badInitialization = true;
//} catch (java.io.EOFException e) {
//  System.out.println("BEScanAvroReader.initialize EOFException failure in '" + filename + "' at " + start);
////System.out.println("BEScanAvroReader.initialize failure in '" + filename + "' at " + ((FileSplit)split).getStart());
//  badInitialization = true;
}
  }

  @Override
//  public boolean nextKeyValue() {
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // call exactly once
    if (isParsed) {
      throw new IOException("Error: BESanAvroReader next already called");
    }

    if (badInitialization) {
      System.out.println("BEScanAvroReader.nextKeyValue skip because of bad initialization in '" + filename + "'");
      return false;
    }


    // parse all records in the split
      try {
    while(reader.hasNext()) {
        BufferRecordReader.BufferRecord record = reader.next();

        // scan the buffer
/*
        String success = scanner.scan(
                          filename,
                          java.math.BigInteger.valueOf(record.offset),
                          "", record.buffer, record.buffer.length);

        if (!success.equals("")) {
          throw new IOException("Error in scan: '" + success + "'");
        }
*/
      //} catch (org.apache.avro.AvroRuntimeException, IOException e) {
    }
      } catch (Exception e) {
        System.out.println("BEScanAvroReader.nextKeyValue abort in '" + filename + "'");
//        break;
      }

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

