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

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

/**
 * Reader interface for reading char[n] from hadoop file bytes.
 * Limitation: It is an IOException to read more than MAX_BUFSIZE at once.
 *
 * Note: This does not read past the hadoop split.
 */
public final class SplitReader extends java.io.Reader {

  private final InputSplit inputSplit;
  private final TaskAttemptContext taskAttemptContext;

  // Hadoop input stream
  private FSDataInputStream in;

  // buffer containing the hadoop split
  private long moreFile;
  private long moreSplit;
  private byte[] buffer;
  private int bufferSize;
  private int bufferHead;

  private SplitReader(InputSplit split,
                     TaskAttemptContext context) {
    inputSplit = split;
    taskAttemptContext = context;
  }

  // open and return a FSDataInputStream
  private void openIN() throws IOException, InterruptedException {

    // open the input file
    final Path path = ((FileSplit)inputSplit).getPath();
    final Configuration configuration = taskAttemptContext.getConfiguration();
    final FileSystem fileSystem = path.getFileSystem(configuration);
    in = fileSystem.open(path);
  }

  // size of file
  public long getFileSize() {
    try {
      final Path path = ((FileSplit)inputSplit).getPath();
      final Configuration configuration = taskAttemptContext.getConfiguration();
      final FileSystem fileSystem = path.getFileSystem(configuration);
      final long fileSize = fileSystem.getFileStatus(path).getLen();
      return fileSize;
    } catch (IOException e) {
      return 0;
    }
  }

  // offset to the start of this split
  public long getSplitOffset() {
    final long splitStart = ((FileSplit)inputSplit).getStart();
    return splitStart;
  }

  // size of this split
  public long getSplitSize() {
    final long splitSize = ((FileSplit)inputSplit).getLength();
    return splitSize;
  }

  // get a reader compatible with java.io.Reader
  public static SplitReader getReader(InputSplit split,
                                      TaskAttemptContext context)
                               throws IOException, InterruptedException {

    // create the reader to return
    SplitReader reader = new SplitReader(split, context);

    // open the reader
    reader.openIN();

    // get offset to start of split
    final long start = reader.getSplitOffset();

    // seek to the split
    reader.in.seek(start);

    // read the split
    final long fileSize = reader.getFileSize();
    final long splitSize = reader.getSplitSize();
    if (start > fileSize) {
      throw new IOException("invalid state");
    }
    reader.bufferSize = (fileSize - start > splitSize) ? (int)splitSize : (int)(fileSize - start);
    reader.buffer = new byte[reader.bufferSize];
    reader.bufferHead = 0;
    org.apache.hadoop.io.IOUtils.readFully(reader.in, reader.buffer, 0, (int)reader.bufferSize);
    return reader;
  }

  // ************************************************************
  // public Reader interfaces
  // ************************************************************

  // close
//zz  public void close() throws IOException, InterruptedException {
  public void close() throws IOException {
    IOUtils.closeStream(in);
//    in.close();
  }

  // do not support marking
  public boolean markSupported() {
    return false;
  }

  public int read(char[] c, int off, int len)
//                      throws IOException, InterruptedException {
                      throws IOException {
//System.out.println("stdout: read: off: " + off + ", len: " + len);
//System.err.println("stderr: read: off: " + off + ", len: " + len);

    // get less than len if at EOF
    final int count = (len < bufferSize - bufferHead) ? len :
                                               bufferSize - bufferHead;

    // no count means EOF
    if (count == 0) {
      return -1;
    }

    // read count from buffer
    for (int i=0; i<count; i++) {
      c[off + i] = (char)(0xff & buffer[bufferHead + i]);
    }

    // move the buffer head forward
    bufferHead += count;

    return count;
  }

  public String readContext(int off, int len) {
    final int start = (off - 16 < 0) ? 0 : off - 16;
    final int stop = (off + len + 16 > bufferSize) ? bufferSize : off+len+16;
    return new String(buffer, start, stop - start);
  }
}

