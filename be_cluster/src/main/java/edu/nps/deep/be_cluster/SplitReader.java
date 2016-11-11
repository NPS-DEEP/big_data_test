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
 * Note: This reads past the hadoop split.  Call splitDistance() to see
 *       how many more bytes can be read before reaching the split.
 */
public final class SplitReader extends java.io.Reader {

  private final InputSplit inputSplit;
  private final TaskAttemptContext taskAttemptContext;

  // Hadoop input stream
  private FSDataInputStream in;

  // buffer between Hadoop and SplitReader output
  private static final int MAX_BUFSIZE = 131072; // 2^17=128KiB
  private long moreFile;
  private long moreSplit;
  private byte[] buffer;
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

  // length of file
  private long getFileLen() throws IOException, InterruptedException {
    final Path path = ((FileSplit)inputSplit).getPath();
    final Configuration configuration = taskAttemptContext.getConfiguration();
    final FileSystem fileSystem = path.getFileSystem(configuration);
    final long fileLen = fileSystem.getFileStatus(path).getLen();
    return fileLen;
  }

  // get a reader compatible with java.io.Reader
  public static SplitReader getReader(InputSplit split,
                                      TaskAttemptContext context)
                               throws IOException, InterruptedException {

    // create the reader to return
    SplitReader reader = new SplitReader(split, context);

    // get offset to start of split
    final long start = ((FileSplit)reader.inputSplit).getStart();

    // open the reader
    reader.openIN();

    // seek to the split
    reader.in.seek(start);

    // set initial values
    reader.moreFile = reader.getFileLen() - start;
    if (reader.moreFile < 0) {
      throw new IOException("invalid state");
    }
    reader.moreSplit = ((FileSplit)reader.inputSplit).getLength();
    reader.buffer = new byte[MAX_BUFSIZE];
    reader.bufferHead = MAX_BUFSIZE;

    return reader;
  }

  // read into buffer if it is too empty
  private void prepareBuffer(int sizeRequested)
                               throws IOException {
//                               throws IOException, InterruptedException {

    // no action if data is available
    int sizeAvailable = MAX_BUFSIZE - bufferHead;
    if (sizeRequested <= sizeAvailable) {
      return;
    }

    // no action if at EOF
    if (moreFile == 0) {
      System.err.println("Note: no prepareBuffer read because moreFile is 0");
      return;
    }

    // impose a max read size less than buffer size
    if (sizeRequested > MAX_BUFSIZE / 2) {
      throw new IOException("invalid state");
    }

    // get count of bytes to read
    int count = bufferHead;
    if (count > moreFile) {
      count = (int)moreFile;
    }

    // shift unread bytes from end to left of bytes to read
    for (int i=bufferHead; i<MAX_BUFSIZE; i++) {
      buffer[i-count] = buffer[i];
    }

    // read count of bytes into buffer
    IOUtils.readFully(in, buffer, bufferHead - count, count);

System.out.println("prepareBuffer.readFully: bufferHead: " + bufferHead + ", count: " + count);

    // adjust tracking variables
    moreFile -= count;
    moreSplit -= count; // goes negative when reading beyond split
    bufferHead -= count;

for (int j=bufferHead; j<MAX_BUFSIZE; j++) {
System.out.println("prepareBuffer.buffer[" + j + "]: " + buffer[j]);
}

  }

  // ************************************************************
  // public Reader interfaces
  // ************************************************************

  // specialized interface
  /**
   * splitDistance becomes 0 and goes negative as we begin to read into
   * the next split.
   */
  public long splitDistance() {
    return moreSplit - (MAX_BUFSIZE - bufferHead);
  }

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
System.out.println("stdout: read: off: " + off + ", len: " + len);
System.err.println("stderr: read: off: " + off + ", len: " + len);

    // require a max read size less than buffer size
    if (len > MAX_BUFSIZE / 2) {
      throw new IOException("invalid usage: " + len +
                            " must be less than " + MAX_BUFSIZE / 2 + ".");
    }

    // make sure buffer is ready to support the read
    prepareBuffer(len);

    // get less than len if at EOF
    final int count = (len < MAX_BUFSIZE - bufferHead) ? len :
                                               MAX_BUFSIZE - bufferHead;

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
}

