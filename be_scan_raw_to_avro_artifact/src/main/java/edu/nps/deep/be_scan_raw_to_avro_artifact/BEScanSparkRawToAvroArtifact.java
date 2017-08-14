package edu.nps.deep.be_scan_raw_to_avro_artifact;

import java.io.IOException;
//import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.Date;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public final class BEScanSparkRawToAvroArtifact {

  // ************************************************************
  // BEScanRawFileInputFormat implements createRecordReader which returns
  // BEScanSplitReader which stores features instead of returning them.
  // ************************************************************
  public static class BEScanRawFileInputFormat
        extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<
                         Long, SerializableArtifact> {

    // createRecordReader returns EmailReader
    @Override
    public org.apache.hadoop.mapreduce.RecordReader<Long, SerializableArtifact>
           createRecordReader(
                 org.apache.hadoop.mapreduce.InputSplit split,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                       throws IOException, InterruptedException {

      BEScanSplitReader reader = new BEScanSplitReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  // ************************************************************
  // Avro Artifact Schema
  // ************************************************************
  private static final String avroArtifactSchemaString =
    "{" +
     "\"namespace\": \"edu.nps.deep.be_scan_raw_to_avro_artifact\"," +
     "\"type\": \"record\"," +
     "\"name\": \"AvroArtifact\"," +
     "\"fields\": [" +
       "{\"name\": \"artifact_class\", \"type\": \"string\"}," +
       "{\"name\": \"stream_name\", \"type\": \"string\"}," +
       "{\"name\": \"recursion_prefix\", \"type\": \"string\"}," +
       "{\"name\": \"offset\", \"type\": \"long\"}," +
       "{\"name\": \"artifact\", \"type\": \"bytes\"}" +
     "]" +
    "}";

  private static final org.apache.avro.Schema avroArtifactSchema = new
     org.apache.avro.Schema.Parser().parse(imageSchemaString);

  // ************************************************************
  // Main
  // ************************************************************

  public static void main(String[] args) {

    if (args.length != 2) {
      System.err.println("Usage: BEScanSparkRawToAvro <directory holding .so files> <input path>");
      System.exit(1);
    }

    // set up the Spark Configuration
    SparkConf sparkConfiguration = new SparkConf();
    sparkConfiguration.setAppName("be_scan Raw to Avro Artifacts");
    sparkConfiguration.set("log4j.logger.org.apache.spark.rpc.akka.ErrorMonitor", "FATAL");
    sparkConfiguration.set("log4j.logger.org.apache.spark.scheduler.DAGScheduler", "TRACE");
    sparkConfiguration.set("yarn.log-aggregation-enable", "true");
    sparkConfiguration.set("fs.hdfs.impl.disable.cache", "true");
    sparkConfiguration.set("spark.app.id", "BEScanSparkRawToAvro App");
//    sparkConfiguration.set("spark.executor.extrajavaoptions", "-XX:+UseConcMarkSweepGC");
    sparkConfiguration.set("spark.dynamicAllocation.maxExecutors", "400");

    sparkConfiguration.set("spark.driver.maxResultSize", "100g"); // default 1g

    sparkConfiguration.set("spark.yarn.executor.memoryOverhead", "4000"); // default 1g

    // set up the Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);

    // make .so libraries available on each node
    sparkContext.addFile(args[0] + "64/" + "libstdc++.so");
    sparkContext.addFile(args[0] + "/" + "libicudata.so");
    sparkContext.addFile(args[0] + "/" + "libicuuc.so");
    sparkContext.addFile(args[0] + "/" + "liblightgrep.so");
    sparkContext.addFile(args[0] + "/" + "liblightgrep_wrapper.so");
    sparkContext.addFile(args[0] + "/" + "libbe_scan.so");
    sparkContext.addFile(args[0] + "/" + "libbe_scan_jni.so");

    try {

      // get the hadoop job
      Job hadoopJob = Job.getInstance(sparkContext.hadoopConfiguration(),
                    "BEScanSparkSQL job");

      // set the Avro Artifact schema
      org.apache.avro.mapreduce.AvroJob.setOutputKeySchema(job,
                                                     avroArtifactSchema);

      // get the hadoop job configuration object
      Configuration configuration = hadoopJob.getConfiguration();

      // get the file system
      FileSystem fileSystem =
                       FileSystem.get(sparkContext.hadoopConfiguration());

      // get the input path
      Path inputPath = new Path(args[1]);

      // iterate over files under the input path to schedule files
      RemoteIterator<LocatedFileStatus> fileStatusListIterator =
                                       fileSystem.listFiles(inputPath, true);
      int i = 0;
      long totalBytes = 0;
      while (fileStatusListIterator.hasNext()) {

        // get file status for this file
        LocatedFileStatus locatedFileStatus = fileStatusListIterator.next();

        // restrict number of files to process else comment this out
        if (++i > 1) {
          break;
        }

        // show this file being added
        System.out.println("adding " + locatedFileStatus.getLen() +
                  " bytes at path " + locatedFileStatus.getPath().toString());

        // add this file to the job
        FileInputFormat.addInputPath(hadoopJob, locatedFileStatus.getPath());
        totalBytes += locatedFileStatus.getLen();

//        // stop after some amount
//        if (totalBytes > 3510000000000L) {
//          break;
//        }
      }

      // Transformation: create the JavaPairRDD of Avro Put Artifacts
      JavaPairRDD<Long, SerializableArtifact> pairRDD =
                                            sparkContext.newAPIHadoopRDD(
               configuration,                        // configuration
               BEScanRawFileInputFormat.class,       // F
               Long.class,                           // K
               SerializableArtifact.class);          // V

System.out.println("BEScanSparkRawToAvroArtifact checkpoint.a");
      // save the JavaPairRDD Artifacts
      pairRDD.saveAsNewAPIHadoopFile("rdc_avro_artifacts1",
                                     AvroKey.class,
                                     NullWritable.class,
                                     AvroKeyOutputFormat.class,
                                     configuration);

System.out.println("BEScanSparkRawToAvroArtifact checkpoint.b");

      // show the total bytes processed
      System.out.println("total bytes processed: " + totalBytes);

      // Done
      System.out.println("Done.");

    }  catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

