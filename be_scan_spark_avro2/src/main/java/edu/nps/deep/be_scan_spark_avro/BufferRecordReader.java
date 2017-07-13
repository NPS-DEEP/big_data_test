package edu.nps.deep.be_scan_spark_avro;

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

import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.FsInput;


/**
 * Reads Avro records of media image data
 */
public final class BufferRecordReader {

  /**
   * The Avro media image schema.
   */
  public final class BufferRecord {

    public final long offset;
    public final byte[] buffer;

    public BufferRecord(long p_offset, byte[] p_buffer) {
      offset = p_offset;
      buffer = p_buffer;
    }
  }

  /**
   * The Avro media image schema.
   */
  private static final String imageSchemaString =
    "{" +
     "\"namespace\": \"edu.nps.deep.be_scan_spark\"," +
     "\"type\": \"record\"," +
     "\"name\": \"AvroSlice\"," +
     "\"fields\": [" +
       "{\"name\": \"offset\", \"type\": \"long\"}," +
       "{\"name\": \"data\", \"type\": \"bytes\"}" +
     "]" +
    "}";

  private static final org.apache.avro.Schema imageSchema = new
     org.apache.avro.Schema.Parser().parse(imageSchemaString);

  private static final DatumReader<GenericRecord> datumReader = new
     GenericDatumReader<GenericRecord>(imageSchema);


//  private final InputSplit inputSplit;
//  private final TaskAttemptContext taskAttemptContext;

  public final String filename;
//  public final long fileSize;
  public final long splitStart;
  public final long splitSize;
  public final DataFileReader<GenericRecord> reader;
  private GenericRecord genericRecord; // object reuse

  public BufferRecordReader(InputSplit inputSplit,
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

//    // fileSize
//    fileSize = fileSystem.getFileStatus(path).getLen();

    // splitStart
    splitStart = ((FileSplit)inputSplit).getStart();

    // splitSize
    splitSize = ((FileSplit)inputSplit).getLength();

    // open the Avro file
    FsInput fsInput = new FsInput(path, configuration);
    reader = new DataFileReader(fsInput, datumReader);

    // move to first sync in split
    reader.sync(splitStart);
  }

  // more if not EOF and not past split
  public boolean hasNext() throws IOException {
    if (!reader.hasNext()) {
      return false;
    }
    return !(reader.pastSync(splitStart + splitSize));
  }

  // read next.  Error if request to start after end of split.
  public BufferRecord next() throws IOException {
    if (!hasNext()) {
      throw new IOException("Error in SplitReader.next: next not available");
    }

    // read the Avro record
    genericRecord = reader.next();
    long offset = (long)genericRecord.get("offset");
    byte[] buffer =
           ((ByteBuffer)genericRecord.get("data")).array();

    // return BufferRecord
    return new BufferRecord(offset, buffer);
  }
}

