
package edu.nps.deep.be_scan_spark_avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.InterruptedException;
import org.apache.spark.SparkFiles;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
//zzimport org.apache.avro.generic.GenericData.Record;

/**
 * The Avro media image schema.
 */
public final class Scan {

  static {
    System.load(SparkFiles.get("libbe_scan.so"));
    System.load(SparkFiles.get("libbe_scan_jni.so"));
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

  private static final Configuration blankConfiguration = new Configuration();

//  private static final int splitSize = 134217728; // 2^27 = 128 MiB

  static void scan(String inFilename)
                        throws IOException, InterruptedException {

    // get file system, which may throw IOException
    final FileSystem fileSystem = FileSystem.get(blankConfiguration);

    // open input
    final Path inPath = new Path(inFilename);
    FSDataInputStream inStream = fileSystem.open(inPath);

//    // size of input file
//    final long inSize = fileSystem.getContentSummary(inPath).getLength();

    // open input
    SeekableInput seekableInput = new FsInput(inPath, blankConfiguration);
    DataFileReader<GenericRecord> dataFileReader = new
                DataFileReader<GenericRecord>(seekableInput, datumReader);

    // create the avro slice object to read into
    GenericRecord avroSlice = null;
    while (dataFileReader.hasNext()) {
      avroSlice = dataFileReader.next(avroSlice);  // support object reuse
      long offset = (long)avroSlice.get("offset");
      byte[] buffer = ((ByteBuffer)avroSlice.get("data")).array();
      scanBufferOldWay(inFilename, buffer, offset);
    }

    // done scanning so close resources
    inStream.close();
    dataFileReader.close();
  }

  // ************************************************************
  // old way
  // ************************************************************
  private static void scanBufferOldWay(String filename,
                                       byte[] buffer, long offset)
                                throws IOException, InterruptedException {

    // open the scanner
    edu.nps.deep.be_scan.BEScan scanner = new edu.nps.deep.be_scan.BEScan(
                       edu.nps.deep.be_scan.be_scan_jni.availableScanners(),
                       buffer, buffer.length);

    // make sure the buffer was allocated
    if (scanner.getBadAlloc()) {
      throw new IOException("memory allocation for scanner buffer failed");
    }

    // read and report artifacts until done
    while (true) {
      edu.nps.deep.be_scan.Artifact artifact = scanner.next();
      if (artifact.getArtifactClass().equals("")) {
        // done
        return;
      }

      // consume this artifact
      String path = String.valueOf(offset + artifact.getBufferOffset());
      show(artifact, path, filename);

      // manage recursion
      if (artifact.hasNewBuffer()) {
        recurse(artifact, path, 1, filename);
        artifact.deleteNewBuffer();
      }
    }
  }

  private static void show(edu.nps.deep.be_scan.Artifact artifact,
                           String path, String filename) {
    byte[] javaArtifact = artifact.javaArtifact();
    byte[] javaContext= artifact.javaContext();

    StringBuilder sb = new StringBuilder();

    sb.append(artifact.getArtifactClass());          // artifact class
    sb.append(" ");                                  // space
    sb.append(filename);                             // filename
    sb.append(" ");                                  // space
    sb.append(path);                                 // path
    sb.append("\t");                                 // tab
    sb.append(escape(artifact.javaArtifact()));      // artifact
    sb.append("\t");                                 // tab
    sb.append(escape(artifact.javaContext()));       // context

    System.out.println("be_scan " + sb.toString());
  }

  private static void recurse(edu.nps.deep.be_scan.Artifact artifactIn,
                       String pathIn, int depth, String filename) {
    // scan the recursed buffer
    edu.nps.deep.be_scan.BEScan recurseScanner =
           new edu.nps.deep.be_scan.BEScan(
           edu.nps.deep.be_scan.be_scan_jni.availableScanners(), artifactIn);
    while(true) {
      edu.nps.deep.be_scan.Artifact artifact = recurseScanner.next();
      if (artifact.getArtifactClass().equals("")) {
        break;
      }

      // compose path
      StringBuilder sb = new StringBuilder();
      sb.append(pathIn);
      sb.append("-");
      sb.append(artifactIn.getArtifactClass().toUpperCase());
      sb.append("-");
      sb.append(String.valueOf(artifact.getBufferOffset()));
      String path = sb.toString();

      // show this artifact
      show(artifact, path, filename);

      // manage recursion
      if (artifact.hasNewBuffer()) {
        if (depth < 7) {
          recurse(artifact, path, depth + 1, filename);
        }
      }

      // release this bufer
      artifact.deleteNewBuffer();
    }
  }

  private static String escape(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<bytes.length; ++i) {
      char c = (char)bytes[i];
      if (c < ' ' || c > '~' || c == '\\') {
        // show as \xXX
        sb.append(String.format("\\x%02X", (int)c&0xff));
      } else {
        // show ascii character
        sb.append(c);
      }
    }
    return sb.toString();
  }

  // ************************************************************
  // new way
  // ************************************************************
  // TBD
}

