// Scan for email addresses, put them in Features.

package edu.nps.deep.be_hbase;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Job;
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
import scala.Tuple2;

/**
 * Reads all email features in one split and puts them in Features.
 */
public final class ScanEmail {

  private final long splitOffset;
  private final long splitSize;
  private final String filename;

  private char[] buffer;

  public Features features;

  public ScanEmail(splitReader) {
    // values from splitReader
    splitOffset = splitReader.getSplitOffset();
    splitSize = splitReader.getSplitSize();
    filename = splitReader.getFilename();

    // the split as char array
    // https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
    char[] buffer = new char[splitSize];

    // storage for the features
    features = new Features();

    splitReader.read(buffer, 0, splitSize);

    for (long i=1; i< splitSize - 1; i++) {
      if (buffer[i] == '@') {
        if (buffer[i+1] != '\0') {
          // unicode 8
          long start = findStart(i);
          if (start == -1) {
            continue;
          }
          long stop = findStop(i);
          if (stop == -1) {
            continue;
          }
          String feature = String(buffer, start, stop-start);
          putFeature(feature, start);

        } else {
          // unicode 16
          long start = findStart16(i);
          if (start == -1) {
            continue;
          }
          long stop = findStop16(i);
          if (stop == -1) {
            continue;
          }
          String feature = string16(start, stop);
          putFeature(feature, start);

        }
      }
    }
  }

  private void putFeature(String feature, long start) {
    features.add(new Feature(filename,
                             Long.toString(start+splitOffset), feature));
  }

  // valid username         = a-z,A-Z,0-9,_,%,-,+,.
  // valid domain           = a-z,A-Z,0-9,_,%,-,+
  // valid top level domain = a-z,A-Z
  // domain separator       = .
  //
  //    %     +     -     .     0-9        A-Z        _     a-z
  //
  //  0x25, 0x2b, 0x2d, 0x2e, 0x30-0x39, 0x41-0x5a, 0x5f, 0x61-0x7a,
  private long findStart(long at) {
    long bottom = at - 1 - 64;
    if (bottom < 0) {
      bottom = 0;
    }
    for (long i = at - 1; i >= bottom; i--) {
      if (buffer[i]
    long start = at



}

