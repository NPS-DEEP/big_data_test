// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.hbase_hash;

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
public final class BlockHashReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         Long, HashRecord> {

  private static final int BLOCK_SIZE = 512;
  private HashRecord hashRecord;
  private SplitReader splitReader;
  private String fileName;
  private long splitOffset;
  private float progress;

  @Override
  public void initialize(
                 org.apache.hadoop.mapreduce.InputSplit split,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                        throws IOException, InterruptedException {

    // open the SplitReader
    splitReader = SplitReader.getReader(split, context);
    fileName = splitReader.getFilename();
    splitOffset = splitReader.getSplitOffset();
    progress = 0.0f;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // get the next hash
    final String blockHexdigest = splitReader.readMD5(BLOCK_SIZE);

    // maybe done
    if (blockHexdigest.length() == 0) {
      progress = 1.0f;
      hashRecord = null;
      return false;
    }

    // create the next hash record
    hashRecord = new HashRecord(fileName, blockHexdigest,
                                Long.toString(splitOffset));
    return true;
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public HashRecord getCurrentValue() throws IOException, InterruptedException {
    return hashRecord;
  }

  @Override
  public float getProgress() throws IOException {
    return progress;
  }

  @Override
  public void close() throws IOException {
    if (splitReader != null) {
      splitReader.close();
    }
  }
}

