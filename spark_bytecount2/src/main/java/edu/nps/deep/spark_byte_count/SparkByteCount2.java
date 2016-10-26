// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.spark_byte_count2;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public final class SparkByteCount2 {

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
        total += histogram[i];
      }
      b.append("\n");
      b.append("total: ");
      b.append(total);
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
                            String, ByteHistogram> {

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
      // not used

      // value

      // get InputSplit in terms of FileSplit
      final org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
                (org.apache.hadoop.mapreduce.lib.input.FileSplit)inputSplit;

      // get path, start, and length
      final org.apache.hadoop.fs.Path path = fileSplit.getPath();
      final long start = fileSplit.getStart();
      final long length = fileSplit.getLength();

System.out.println("SplitFileRecordReader.initialize path: " +
                   path + ", start: " + start + ", length: " + length);

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
    public String getCurrentKey() throws IOException, InterruptedException {
      return "not used";
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
                         String, ByteHistogram> {

    // createRecordReader returns SplitFileRecordReader
    @Override
    public org.apache.hadoop.mapreduce.RecordReader<
                         String, ByteHistogram>
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
      System.err.println("Usage: SparkByteCount2 <input path>");
      System.exit(1);
    }

    // set up the Spark Configuration
    SparkConf sparkConfiguration = new SparkConf();
    sparkConfiguration.setAppName("Spark Byte Count App");
    //sparkConfiguration.set("log4j.logger.org.apache.spark.rpc.akka.ErrorMonitor", "FATAL");
    sparkConfiguration.set(
               "log4j.logger.org.apache.spark.rpc.akka.ErrorMonitor", "TRACE");

    // set up the Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);

    // several hadoop functions return IOException
    try {

      // get the hadoop job
      Job hadoopJob = Job.getInstance(
                       sparkContext.hadoopConfiguration(), "SparkByteCount2");

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

        // restrict number of files to process or else comment this out
        if (++i > 10) {
          break;
        }

        System.out.println("adding " + locatedFileStatus.getLen() +
                  " bytes at path " + locatedFileStatus.getPath().toString());


        // add this file
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(
                                   hadoopJob, locatedFileStatus.getPath());

        totalBytes += locatedFileStatus.getLen();
      }
      System.out.println("total bytes added: " + totalBytes);

      // create the RDD of byte histograms for splits
      JavaPairRDD<String, ByteHistogram> rdd = sparkContext.newAPIHadoopRDD(
               hadoopJob.getConfiguration(),        // configuration
               SplitFileInputFormat.class,          // F
               String.class,                        // K
               ByteHistogram.class);                // V

      // reduce RDD to total result
      Tuple2<String, ByteHistogram> histogramTotalTuple = rdd.reduce(
            new Function2<Tuple2<String, ByteHistogram>,
                         Tuple2<String, ByteHistogram>,
                         Tuple2<String, ByteHistogram>>() {
        @Override
        public Tuple2<String, ByteHistogram> call(
                                Tuple2<String, ByteHistogram> v1,
                                Tuple2<String, ByteHistogram> v2) {
          ByteHistogram v3 = new ByteHistogram();
          v3.add(v1._2());
          v3.add(v2._2());
          return new Tuple2<String, ByteHistogram>("not used 2", v3);
        }
      });

      // show histogram total
      System.out.println("Histogram total:\n" +
                         histogramTotalTuple._2());

    }  catch (IOException e) {
      System.err.println("Error starting main: " + e);
      System.exit(1);
    }
  }
}

