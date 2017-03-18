package edu.nps.deep.be_scan_spark;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
 * Reads the byte[] buffer of split size at the hdfs file split.
 */
public final class SplitReader {

  private final InputSplit inputSplit;
  private final TaskAttemptContext taskAttemptContext;

  public final Configuration configuration;
  public final String filename;
  public final long fileSize;
  public final long splitStart;
  public final long splitSize;
  public final byte[] buffer;

  public SplitReader(InputSplit split, TaskAttemptContext context)
                               throws IOException, InterruptedException {

    // configuration
    configuration = taskAttemptContext.getConfiguration();

    // hadoop path
    final Path path = ((FileSplit)inputSplit).getPath();

    // hadoop filesystem
    final FileSystem fileSystem = path.getFileSystem(configuration);

    // filename
    filename = path.toString();

    // fileSize
    fileSize = fileSystem.getFileStatus(path).getLen();

    // splitStart
    splitStart = ((FileSplit)inputSplit).getStart();

    // splitSize
    splitSize = ((FileSplit)inputSplit).getLength();

    // open the input file
    FSDataInputStream in = fileSystem.open(path);

    // seek to the split
    reader.in.seek(splitStart);

    // start should be valid
    if (splitStart > fileSize) {
      throw new IOException("invalid state");
    }

    // bufferSize
    int bufferSize = (fileSize - splitStart > splitSize) ? (int)splitSize : (int)(fileSize - start);
    buffer = new byte[reader.bufferSize];
    org.apache.hadoop.io.IOUtils.readFully(reader.in, buffer, 0, bufferSize);
    IOUtils.closeStream(in);
  }
}

