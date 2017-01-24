// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

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
 * Reads all email features in one split and returns them in one call
 * to nextKeyValue().
 */
public final class EmailReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         Long, Feature> {

  private Features features;
  private Feature feature;
  private boolean isParsed = false;
  private Iterator<Feature> iterator = features.iterator();
  private SplitReader splitReader;

  @Override
  public void initialize(
                 org.apache.hadoop.mapreduce.InputSplit split,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                        throws IOException, InterruptedException {

    // open the SplitReader
    splitReader = SplitReader.getReader(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // maybe parse the split into features
    if (!isParsed) {
      // parse the whole split and capture all email features
      BinaryLexer l = new BinaryLexer(splitReader);
      do {
        l.yylex();
      } while (!l.at_eof());

      // make reference to the found email features
      features = l.features;
      iterator = features.iterator();

      // now parsed
      isParsed = true;
    }

    // maybe done
    if (iterator.hasNext() == false) {
      return false;
    }

    // stage the next feature
    feature = iterator.next();
    return true;
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public Feature getCurrentValue() throws IOException, InterruptedException {
    return feature;
  }

  @Override
  public float getProgress() throws IOException {
    return (iterator.hasNext()) ? 0.0f : 1.0f;
  }

  @Override
  public void close() throws IOException {
    if (splitReader != null) {
      splitReader.close();
    }
  }
}

