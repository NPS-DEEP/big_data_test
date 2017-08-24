package edu.nps.deep.regroup_avro_artifacts;

import java.io.IOException;
//import java.io.File;
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
import org.apache.hadoop.io.NullWritable;

//import org.apache.avro.mapred.AvroKey;
//import org.apache.avro.generic.GenericRecord;

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

public final class SparkRegroupAvroArtifacts {

  // ************************************************************
  // Main
  // ************************************************************

  public static void main(String[] args) {

    if (args.length != 2) {
      System.err.println("Usage: SparkRegroupAvroArtifacts <input path> <output path>");
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

    sparkConfiguration.set("spark.driver.maxResultSize", "100g"); // default 1g

    sparkConfiguration.set("spark.yarn.executor.memoryOverhead", "4000"); // default 1g

    // set up the Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);

    try {

      // get the hadoop job
      Job hadoopJob = Job.getInstance(sparkContext.hadoopConfiguration(),
                    "SparkRegroupAvroArtifacts");

      // set the Avro Artifact schema
      org.apache.avro.mapreduce.AvroJob.setOutputKeySchema(hadoopJob,
                                   AvroArtifactSchema.avroArtifactSchema);

      // get the hadoop job configuration object
      Configuration configuration = hadoopJob.getConfiguration();

      // get the file system
      FileSystem fileSystem =
                       FileSystem.get(sparkContext.hadoopConfiguration());

      // get the input path
      Path inputPath = new Path(args[0]);

      // get the output path
      Path outputPath = new Path(args[1]);

System.out.println("SparkRegroupAvroArtifacts.checkpoint.a");
      // read Avro into JavaPairRDD
      JavaPairRDD rdd1 = sparkContext.hadoopFile(
               inputPath,
               BEScanRawFileInputFormat.class,       // F
               AvroKeyHack.class,                    // K
               NullWritable.class);                  // V

      // regroup Avro to 500
      JavaPairRDD rdd2 = rdd1.groupByKey(500);

System.out.println("SparkRegroupAvroArtifacts.checkpoint.b");
      // save the JavaPairRDD Artifacts
      pairRDD.saveAsNewAPIHadoopFile(outputPath,
                     AvroKeyHack.class,
                     NullWritable.class,
                     org.apache.avro.mapreduce.AvroKeyOutputFormat.class,
                     configuration);

System.out.println("SparkRegroupAvroArtifacts.checkpoint.c");

      // Done
      System.out.println("Done.");

    }  catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

