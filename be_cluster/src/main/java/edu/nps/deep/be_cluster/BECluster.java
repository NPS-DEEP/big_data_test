// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.be_cluster;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.ArrayDeque;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

//zz import EmailReader;

public final class BECluster {

  // ************************************************************
  // SplitFileInputFormat implements createRecordReader which returns
  // EmailReader for extracting email addresses from one split.
  // ************************************************************
  public static class SplitFileInputFormat
        extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<
                         Long, ArrayDeque<ExtractedFeature>> {

    // createRecordReader returns EmailReader
    @Override
    public org.apache.hadoop.mapreduce.RecordReader<
                         Long, ArrayDeque<ExtractedFeature>>
           createRecordReader(
                 org.apache.hadoop.mapreduce.InputSplit split,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                       throws IOException, InterruptedException {

//      org.apache.hadoop.mapreduce.RecordReader<Long, ArrayDeque<ExtractedFeature>> reader = new EmailReader();
      EmailReader reader = new EmailReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  // ************************************************************
  // feature recorder VoidFunction
  // ************************************************************
  public static class FeatureRecorderVoidFunction
            implements VoidFunction<ArrayDeque<ExtractedFeature>> {

    private java.io.BufferedWriter out;

    public FeatureRecorderVoidFunction(java.io.File featureFile) {
      try {
        out = new java.io.BufferedWriter(new java.io.FileWriter(featureFile));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void call(ArrayDeque<ExtractedFeature> features) {
      while (features.size() != 0) {
        ExtractedFeature feature = features.remove();
        out.write(feature.forensicPath + "\t" + feature.featureBytes);
      }
    }
  }

  // ************************************************************
  // Main
  // ************************************************************

  public static void main(String[] args) {

    if (args.length != 1) {
      System.err.println("Usage: BECluster <input path>");
      System.exit(1);
    }

    // set up the Spark Configuration
    SparkConf sparkConfiguration = new SparkConf();
    sparkConfiguration.setAppName("Spark Byte Count App");
    sparkConfiguration.set("log4j.logger.org.apache.spark.rpc.akka.ErrorMonitor", "FATAL");
    sparkConfiguration.set("log4j.logger.org.apache.spark.scheduler.DAGScheduler", "TRACE");
    sparkConfiguration.set("yarn.log-aggregation-enable", "true");
    sparkConfiguration.set("fs.hdfs.impl.disable.cache", "true");
    sparkConfiguration.set("spark.app.id", "Spark Byte Count 2 App");
    sparkConfiguration.set("spark.executor.extrajavaoptions", "-XX:+UseConcMarkSweepGC");
    sparkConfiguration.set("spark.dynamicAllocation.maxExecutors", "10000");

// no, we will have multiple keys:    sparkConfiguration.set("spark.default.parallelism", "1");
    sparkConfiguration.set("spark.default.parallelism", "64");

    sparkConfiguration.set("spark.driver.maxResultSize", "8g"); // default 1g, may use 2.5g

    // create the local output directory as output+timestamp
    java.io.File localOutputDirectory = new java.io.File("output" + new SimpleDateFormat(
                          "yyyy-MM-dd hh-mm-ss'.tsv'").format(new Date()));
    localOutputDirectory.mkdir();

    // future actions for each job
    ArrayDeque<JavaFutureAction<Void>> javaFutureActions = new
                                  ArrayDeque<JavaFutureAction<Void>>();

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
//if (locatedFileStatus.getPath().toString().indexOf("Fedora-Xfce-Live-x86_64-24-1.2.iso") >= 0) {
//continue;
//}

        // show file being added
        System.out.println("adding " + locatedFileStatus.getLen() +
                  " bytes at path " + locatedFileStatus.getPath().toString());

        // get a hadoop job
        Job hadoopJob = Job.getInstance(sparkContext.hadoopConfiguration(),
                    "Job for file " + locatedFileStatus.getPath().toString());

        // add this file to the job
        FileInputFormat.addInputPath(hadoopJob, locatedFileStatus.getPath());

        // define the RDD of byte histograms for splits for this job
        JavaPairRDD<Long, ArrayDeque<ExtractedFeature>> rdd = sparkContext.newAPIHadoopRDD(
               hadoopJob.getConfiguration(),         // configuration
               SplitFileInputFormat.class,           // F
               Long.class,                           // K
//               ArrayDeque<ExtractedFeature>.class);  // V
               ArrayDeque.class);  // V

        // create the recorder that will write this RDD to a local file
        java.io.File featureFile = new java.io.File(localOutputDirectory,
                               locatedFileStatus.getPath().getName());
        FeatureRecorderVoidFunction recorder =
                               new FeatureRecorderVoidFunction(featureFile);

        // create the JavaFutureAction for this job
        JavaFutureAction<Void> f = rdd.foreachAsync(recorder);
        javaFutureActions.add(f);
      }

      // show the total bytes being processed
      System.out.println("total bytes added: " + totalBytes);

      // wait for all futures to finish.
      while (javaFutureActions.size() != 0) {
        JavaFutureAction<Void> f = javaFutureActions.remove();
      }

      // Done
      System.out.println("Done.");

    }  catch (IOException|InterruptedException|ExecutionException e) {
      throw new RuntimeException(e);
//      System.exit(1);
    }
  }
}

