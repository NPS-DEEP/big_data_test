// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.spark_byte_count;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public final class SparkByteCount {

  // ************************************************************
  // ByteHistogram contains a histogram distribution of bytes.
  // ************************************************************
  static class ByteHistogram implements java.io.Serializable {
    public long[] histogram = new long[256];

    public ByteHistogram() {
      histogram = new long[256];
    }

    public void add(byte[] bytes) {
      for (byte b : bytes) {
        ++histogram[b&0xff];
      }
    }

    public void add(ByteHistogram other) { 
      for (int i = 0; i< histogram.length; i++) {
        histogram[i] += other.histogram[i];
      }
    }

    public String toString() {
      StringBuilder b = new StringBuilder();
      long total = 0;
      for (int i=0; i<256; i++) {
        b.append(i);
        b.append(" ");
        b.append(histogram[i]);
        b.append(" ");
        total += histogram[i];
      }
      b.append("\n");
      b.append("total: ");
      b.append(total);
      b.append("\n");
      return b.toString();
    }

/*
    public String toString() {
      StringBuilder b = new StringBuilder();
      long total = 0;
      for (int i=0; i<256; i++) {
        b.append(i);
        b.append(" ");
        b.append(histogram[i]);
        b.append("\n");
        total += histogram[i];
      }
      b.append("total: ");
      b.append(total);
      return b.toString();
    }
*/
  }

  // ************************************************************
  // SplitFileReacordReader reads the requested split and returns ByteHistogram.
  // ref. https://github.com/apache/mahout/blob/master/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java
  // ************************************************************
  static class SplitFileRecordReader
                            extends org.apache.hadoop.mapreduce.RecordReader<
                            Long, ByteHistogram> {

    private org.apache.hadoop.mapreduce.InputSplit inputSplit;
    private org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext;

    private ByteHistogram byteHistogram = new ByteHistogram();
    private boolean isDone = false;

    @Override
    public void initialize(
                 org.apache.hadoop.mapreduce.InputSplit _inputSplit,
                 org.apache.hadoop.mapreduce.TaskAttemptContext _context)
                        throws IOException, InterruptedException {

      this.inputSplit = _inputSplit;
      this.taskAttemptContext = _context;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

      // done when no more bytes to read in split
      if (isDone) {
        return false;
      }

      // key
      // use Long(1)

      // value

      // get InputSplit in terms of FileSplit
      final org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
                (org.apache.hadoop.mapreduce.lib.input.FileSplit)inputSplit;

      // get path, start, and length
      final org.apache.hadoop.fs.Path path = fileSplit.getPath();
      final long start = fileSplit.getStart();
      final long length = fileSplit.getLength();

/* zz none of these work
org.apache.log4j.LogManager.getRootLogger().error("zzzzzzz1 " +
                    path + ", start: " + start + ", length: " + length);

org.apache.log4j.Logger.getLogger(SparkByteCount.class.getName()).error("zzzzzzz2 " +
                   path + ", start: " + start + ", length: " + length);

System.out.println("SplitFileRecordReader.initialize path(zzzz3): " +
                   path + ", start: " + start + ", length: " + length);
*/

      // open the input file
      final org.apache.hadoop.conf.Configuration configuration =
                                   taskAttemptContext.getConfiguration();
      final org.apache.hadoop.fs.FileSystem fileSystem =
                                   path.getFileSystem(configuration);
      org.apache.hadoop.fs.FSDataInputStream in = fileSystem.open(path);

      // seek to the split
      in.seek(start);

      // iteratively read the partition
      final long maxStep = 131072; // 2^17=128KiB
      long more = fileSplit.getLength();
      while (more > 0) {
        long count = (more > maxStep) ? maxStep : more;
        byte[] contents = new byte[(int)count];
        org.apache.hadoop.io.IOUtils.readFully(in, contents, 0, (int)count);
        byteHistogram.add(contents);
        more -= count;
      }

      // done with this partition
      org.apache.hadoop.io.IOUtils.closeStream(in);
      isDone = true;
      return true;
    }

    @Override
    public Long getCurrentKey() throws IOException, InterruptedException {
      return new Long(1);
    }

    @Override
    public ByteHistogram getCurrentValue()
                                  throws IOException, InterruptedException {
      return byteHistogram;
    }

    @Override
    public float getProgress() throws IOException {
      return (isDone == true) ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
      // no action
    }
  }

  // ************************************************************
  // SplitFileInputFormat implements createRecordReader which returns
  // SplitFileRecordReader for calculating ByteHistogram for one split.
  // ************************************************************
  public static class SplitFileInputFormat
        extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<
                         Long, ByteHistogram> {

    // createRecordReader returns SplitFileRecordReader
    @Override
    public org.apache.hadoop.mapreduce.RecordReader<
                         Long, ByteHistogram>
           createRecordReader(
                 org.apache.hadoop.mapreduce.InputSplit split,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                       throws IOException, InterruptedException {

      SplitFileRecordReader reader = new SplitFileRecordReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  // ************************************************************
  // Main
  // ************************************************************

  public static void main(String[] args) {

    if (args.length != 1) {
      System.err.println("Usage: SparkByteCount <input path>");
      System.exit(1);
    }

    // set up the Spark Configuration
    SparkConf sparkConfiguration = new SparkConf();
    sparkConfiguration.setAppName("Spark Byte Count App");
    sparkConfiguration.set("log4j.logger.org.apache.spark.rpc.akka.ErrorMonitor", "FATAL");
    sparkConfiguration.set("log4j.logger.org.apache.spark.scheduler.DAGScheduler", "TRACE");
    sparkConfiguration.set("fs.hdfs.impl.disable.cache", "true");
    sparkConfiguration.set("spark.app.id", "Spark Byte Count 2 App");
    sparkConfiguration.set("spark.executor.extrajavaoptions", "-XX:+UseConcMarkSweepGC");
    sparkConfiguration.set("spark.dynamicAllocation.maxExecutors", "10000");

//// no, we will have multiple keys:    sparkConfiguration.set("spark.default.parallelism", "1");
//    sparkConfiguration.set("spark.default.parallelism", "64");
//    sparkConfiguration.set("spark.driver.maxResultSize", "8g"); // default 1g, may use 2.5g

    // future actions for each job
    ArrayDeque<JavaFutureAction<java.util.List<Tuple2<Long, ByteHistogram>>>>
                  javaFutureActions = new ArrayDeque<JavaFutureAction<
                       java.util.List<Tuple2<Long, ByteHistogram>>>>();

    // set up the Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);

    // several hadoop functions return IOException
    try {

      // get the file system
      FileSystem fileSystem =
                       FileSystem.get(sparkContext.hadoopConfiguration());

      // get the input path
      Path inputPath = new Path(args[0]);

      // iterate over files under the input path
      RemoteIterator<LocatedFileStatus> fileStatusListIterator =
                                       fileSystem.listFiles(inputPath, true);
      int i = 0;
      long totalBytes = 0;
      while (fileStatusListIterator.hasNext()) {

        // get file status for this file
        LocatedFileStatus locatedFileStatus = fileStatusListIterator.next();

//        // restrict number of files to process or else comment this out
//        if (++i > 10) {
//          break;
//        }

        // show file being added
        System.out.println("adding " + locatedFileStatus.getLen() +
                  " bytes at path " + locatedFileStatus.getPath().toString());

        // get a hadoop job
        Job hadoopJob = Job.getInstance(sparkContext.hadoopConfiguration(),
                    "Job for file " + locatedFileStatus.getPath().toString());

        // add this file to the job
        FileInputFormat.addInputPath(hadoopJob, locatedFileStatus.getPath());

        // define the RDD of byte histograms for splits for this job
        JavaPairRDD<Long, ByteHistogram> rdd = sparkContext.newAPIHadoopRDD(
               hadoopJob.getConfiguration(),        // configuration
               SplitFileInputFormat.class,          // F
               Long.class,                          // K
               ByteHistogram.class);                // V

        // define the reduced RDD for this job
        JavaPairRDD<Long, ByteHistogram> rdd2 = rdd.reduceByKey(
            new Function2< ByteHistogram, ByteHistogram, ByteHistogram>() {
          @Override
          public ByteHistogram call(ByteHistogram v1, ByteHistogram v2) {

            // add second byteHistogram to first
            v1.add(v2);
            return v1;
          }
        });

        // create the JavaFutureAction for this job
        JavaFutureAction<java.util.List<Tuple2<Long, ByteHistogram>>> f =
                                                        rdd2.collectAsync();
        javaFutureActions.add(f);
      }

      // show the total bytes being processed
      System.out.println("total bytes added: " + totalBytes);

      // wait for all futures to finish, totaling as jobs complete
      ByteHistogram total = new ByteHistogram();
      while (javaFutureActions.size() != 0) {
        JavaFutureAction<java.util.List<Tuple2<Long, ByteHistogram>>> j =
                                                   javaFutureActions.remove();
        java.util.List<Tuple2<Long, ByteHistogram>> list = j.get();
        if (list.size() != 1) {
          throw new RuntimeException("bad");
        }
        total.add(list.get(0)._2());
      }

      // show histogram total
      System.out.println("Histogram total:\n" + total);

      // save total in text file
      java.io.File totalFile = new java.io.File("temp_total_textfile");
      try {
        java.io.BufferedWriter out = new java.io.BufferedWriter(new java.io.FileWriter(totalFile));
        out.write("Histogram total:\n" + total);
        out.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }  catch (IOException|InterruptedException|ExecutionException e) {
      throw new RuntimeException(e);
//      System.exit(1);
    }
  }
}

