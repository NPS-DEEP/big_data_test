package edu.nps.deep.be_scan_raw_to_avro_artifact;

import org.apache.avro.Schema;

public final class AvroArtifactSchema {

  // ************************************************************
  // Avro Artifact Schema
  // ************************************************************
  private static final String avroArtifactSchemaString =
    "{" +
     "\"namespace\": \"edu.nps.deep.be_scan_raw_to_avro_artifact\"," +
     "\"type\": \"record\"," +
     "\"name\": \"AvroArtifact\"," +
     "\"fields\": [" +
       "{\"name\": \"artifact_class\", \"type\": \"string\"}," +
       "{\"name\": \"stream_name\", \"type\": \"string\"}," +
       "{\"name\": \"recursion_prefix\", \"type\": \"string\"}," +
       "{\"name\": \"offset\", \"type\": \"long\"}," +
       "{\"name\": \"artifact\", \"type\": \"bytes\"}" +
     "]" +
    "}";

  public static final org.apache.avro.Schema avroArtifactSchema = new
     org.apache.avro.Schema.Parser().parse(avroArtifactSchemaString);
}

