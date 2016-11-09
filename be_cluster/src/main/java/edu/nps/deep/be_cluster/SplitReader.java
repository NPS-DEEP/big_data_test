package edu.nps.deep.be_cluster;

/*
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
*/

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public final class SplitReader extends java.io.Reader {

  private InputSplit inputSplit;
  private TaskAttemptContext taskAttemptContext;

  private SplitReader(InputSplit split,
                     TaskAttemptContext context) {
    inputSplit = split;
    taskAttemptContext = context;
  }

  public SplitReader getReader(InputSplit split, TaskAttemptContext context)
                               throws IOException, InterruptedException {
    SplitReader reader = new SplitReader(split, context);



    // get InputSplit in terms of FileSplit
    final FileSplit fileSplit = (FileSplit)inputSplit;

    // get path, start, and length
    final org.apache.hadoop.fs.Path path = fileSplit.getPath();
    final long start = fileSplit.getStart();
    final long length = fileSplit.getLength();


}

