// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.image_to_avro;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Copy on first call to nextKeyValue.
 * Nothing meaningful is returned for the RDD and returned constants
 * should be changed to null.
 */
public final class RawToAvroReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         Long, String> {

  private String inputFilename;
  private String outputFilename;
  private boolean isCopied = false;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
                                throws IOException, InterruptedException {

    // get the filename string for reporting artifacts
    inputFilename = ((FileSplit)split).getPath().toString();

    // compose the output filename using a hardcoded prefix
    outputFilename = "null2/" + ((FileSplit)split).getPath().getName();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // call exactly once
    if (isCopied) {
      throw new IOException("Error: RawToAvroReader next already called");
    }

    // copy to Avro
    CopyImageToAvro.rawToAvro(inputFilename, outputFilename);
    isCopied = true;
    return false;
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return new Long(1);
  }

  @Override
  public String getCurrentValue() throws IOException, InterruptedException {
    return "done";
  }

  @Override
  public float getProgress() throws IOException {
    return isCopied ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // no action
  }
}


