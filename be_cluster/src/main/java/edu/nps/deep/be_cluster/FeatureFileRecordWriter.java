// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.be_cluster;

import java.io.IOException;
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
 * Reads all email features in one split and returns them in one call
 * to nextKeyValue().
 */
public final class FeatureFileRecordWriter
                         extends org.apache.hadoop.mapreduce.RecordWriter<
                         Long, Features> {

  @Override
  public void write(Long unusedKey, Features features)
                        throws IOException, InterruptedException {

    while (features.size() != 0) {
      Feature feature = features.get();
      zzzzzzzzzzzzzzzzzzzzzzzzz

    // open the SplitReader
    splitReader = SplitReader.getReader(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // only call this once
    if (isDone) {
      return false;
    } else {
      isDone = true;
    }

    // parse the whole split and capture all email features
    BinaryLexer l = new BinaryLexer(splitReader,
              splitReader.getSplitOffset(), splitReader.getSplitSize());
    do {
      l.yylex();
    } while (!l.at_eof());

    // done if no features found
    if (features.size() == 0) {
      return false;
    }

    // keep the found email features
    features = l.features;

    return true;
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public Features getCurrentValue()
                              throws IOException, InterruptedException {
    return features;
  }

  @Override
  public float getProgress() throws IOException {
    return (isDone == true) ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    if (splitReader != null) {
      splitReader.close();
    }
  }
}

