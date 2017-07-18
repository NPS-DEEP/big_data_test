package edu.nps.deep.be_scan_spark2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

/**
 * Reads buffer records of media image data
 */
public final class BufferReader {

  /**
   * The buffer record.
   */
  public final class BufferRecord {

    public final long offset;
    public final byte[] buffer;

    public BufferRecord(long p_offset, byte[] p_buffer) {
      offset = p_offset;
      buffer = p_buffer;
    }
  }

  private final int bufferSize = 65536;
//  private final int bufferSize = 1024;
  private byte[] buffer = new byte[bufferSize];
  private FSDataInputStream in;

  private final String filename;
  private final long fileSize;
  private final long splitStart; // offset from start of file
  private final long splitSize;
  private int innerSplitOffset; // offset into split

  public BufferReader(InputSplit inputSplit,
                      TaskAttemptContext taskAttemptContext)
                               throws IOException, InterruptedException {

    // configuration
    final Configuration configuration = taskAttemptContext.getConfiguration();

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

    // open the HDFS binary file
    in = fileSystem.open(path);

    // move to split
    in.seek(splitStart);
    innerSplitOffset = 0;
  }

  // more if not EOF and not past split
  public boolean hasNext() throws IOException {
    // done when at end of split or EOF
    if (innerSplitOffset >= splitSize ||
                  splitStart + innerSplitOffset == fileSize) {
      return false;
    } else {
      return true;
    }
  }

  // read next.  Error if request to start after end of split.
  public BufferRecord next() throws IOException {
    if (!hasNext()) {
      throw new IOException("Error in SplitReader.next: next not available");
    }

    // get number of bytes to read
    int count = bufferSize;
    if (splitStart + innerSplitOffset + count > fileSize) {
      count = (int)(fileSize - splitStart - innerSplitOffset);
    }
    if (innerSplitOffset + count > splitSize) {
      count = (int)(splitSize - innerSplitOffset);
    }

    // read the buffer from the split
    org.apache.hadoop.io.IOUtils.readFully(in, buffer, 0, count);

    // compose BufferRecord
    BufferRecord bufferRecord = new BufferRecord(
                                splitStart + innerSplitOffset, buffer);

    innerSplitOffset += count;
    return bufferRecord;
  }
}

