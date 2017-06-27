
package edu.nps.deep.image_to_avro;

import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
//zzimport org.apache.avro.generic.GenericData.Record;

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

  private static final FileSystem fileSystem =
                              FileSystem.get(new Configuration());
  private static final long splitSize = 134217728; // 2^27 = 128 MiB

  static void rawToAvro(String inFilename, String outFilename)
                        throws IOException, InterruptedException {

    // open input
    final Path inPath = new Path(inFilename);
    FSDataInputStream inStream = fileSystem.open(inPath);

    // size of input file
    final long inSize = fileSystem.getContentSummary(inPath).getLength();

    // open output, false throws exception if file already exists
    final Path outPath = new Path(outFilename);
    FSDataOutputStream outStream = fileSystem.create(outPath, false);
    DataFileWriter<GenericRecord> dataFileWriter = new
                                DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(imageSchema, outStream);

    // create a byte buffer
    int bufferSize = (inSize > splitSize) ? splitSize : (int)inSize;
    byte[] buffer = new byte[bufferSize];

    // create the avro output record
    GenericRecord avroSlice = new GenericData.Record(imageSchema);

    // iterate across the image
    long offset = 0;

    while (offset != inSize) {

      // get count to read
      int count = (inSize - offset > bufferSize ? bufferSize :
                                                  (int)(inSize - offset));

      // read inFile into buffer
      inStream.readFully(offset, buffer, 0, count);

      // write buffer to outFile
      avroSlice.put("offset", offset);
      avroSlice.put("data", buffer);

      // next
      offset += count;
    }

    // done copying so close resources
    in.close();
    dataFileWriter.close();
  }
}

