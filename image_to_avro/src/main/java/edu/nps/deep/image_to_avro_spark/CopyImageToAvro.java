
package edu.nps.deep.image_to_avro;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

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

  private static final DatumWriter<GenericRecord> dataumWriter = new
     GenericDatumWriter<GenericRecord>(GenericRecord.class);

  private static final FileSystem fileSystem =
                              FileSystem.get(new Configuration());
  private static final long splitSize = 134217728; // 2^27 = 128 MiB

  private AvroSlice avroSlice = new AvroSlice();

  private static rawToAvro(String inFilename, String outFilename) {
                               throws IOException, InterruptedException {

    // open input
    final Path inPath = Path(inFilename);
    FSDataInputStream in;
    try {
      in = fileSystem.open(inPath);
    } catch (IOException e) {
      System.out.println("RawToAvro read error in fileSystem.open");
      throw e;
    }

    // size of input file
    final inSize = fileSystem.getContentSummary(inPath).getLength();

    // open output, false throws exception if file already exists
    final Path outPath = Path(outFilename);
    FSDataOutputStream outStream = fileSsytem.create(outPath, false);
    DataFileWriter<GenericRecord> dataFileWriter = new
                                DataFileWriter<GenericRecord>(datumWriter);
    datafileWriter.create(imageSchema, outStream);

    // create the avro output record
    GenericRecord avroSlice = new GenericData.Record(imageSchema);

    // iterate across the image
    long offset = 0;

    while (true) {

      // create a byte buffer
      long remaining = inSize - offset;
      int bufferSize = remaining > inSize ? (int)inSize : (int)remaining;
      buffer = new byte[bufferSize];

      // read inFile into buffer
      if (bufferSize == 0) {
        // done
        break;
      }
      org.apache.hadoop.io.IOUtils.readFully(in, buffer, offset, bufferSize);

      // write buffer to outFile
      avroSlice.put("offset", offset);
      avroSlice.put("data", buffer);
    }

    // done copying so close resources
    in.close();
    dataFileWriter.close();
  }
}

