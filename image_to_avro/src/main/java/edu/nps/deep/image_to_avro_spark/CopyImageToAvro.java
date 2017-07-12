
package edu.nps.deep.image_to_avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;

/**
 * The Avro media image schema.
 */
public final class CopyImageToAvro {

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

  private static final DatumWriter<GenericRecord> datumWriter = new
     GenericDatumWriter<GenericRecord>(imageSchema);

  private static final Configuration blankConfiguration = new Configuration();

//zz  private static final int splitSize = 134217728; // 2^27 = 128 MiB
  private static final int bufferSize = 65536;

  static void rawToAvro(String inFilename, String outFilename)
                        throws IOException, InterruptedException {

    // get file system, which may throw IOException
    final FileSystem fileSystem = FileSystem.get(blankConfiguration);

    // open input
    final Path inPath = new Path(inFilename);
    FSDataInputStream inStream = fileSystem.open(inPath);

    // size of input file
    final long inSize = fileSystem.getContentSummary(inPath).getLength();

    // open output, use false to throw exception if file already exists
    final Path outPath = new Path(outFilename);
    FSDataOutputStream outStream = fileSystem.create(outPath, false);
    DataFileWriter<GenericRecord> dataFileWriter = new
                                DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.setCodec(CodecFactory.snappyCodec());
    dataFileWriter.setSyncInterval(65536);
    dataFileWriter.create(imageSchema, outStream);

    // create a byte buffer
    byte[] buffer = new byte[bufferSize];

    // create the avro output record
    GenericRecord avroSlice = new GenericData.Record(imageSchema);

    // iterate across the image
    long offset = 0;

    long oldCurrentPosition = 0;
    while (offset != inSize) {

      // get count to read
      int count = (inSize - offset > bufferSize ? bufferSize :
                                                  (int)(inSize - offset));

      // read inFile into buffer
      inStream.readFully(offset, buffer, 0, count);

      // write buffer to outFile
      avroSlice.put("offset", offset);
      avroSlice.put("data", ByteBuffer.wrap(buffer, 0, count));
System.out.println("Append " + count + " of " + inSize + " at offset " + offset + " to " + outFilename);
      dataFileWriter.append(avroSlice);

      // next
      offset += count;

//zz diagnostic
long currentPosition = dataFileWriter.sync();
System.out.println("sync " + inFilename + " " + currentPosition + "  " + (currentPosition - oldCurrentPosition));
oldCurrentPosition = currentPosition;
    }

    // done copying so close resources
    inStream.close();
    dataFileWriter.close();
    outStream.close();
  }
}

