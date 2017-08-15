package edu.nps.deep.be_scan_raw_to_avro_artifact;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.generic.GenericRecord;

import edu.nps.deep.be_scan.Artifact;

public final class AvroKeyHack extends AvroKey<GenericRecord> {

  AvroKeyHack(GenericRecord genericRecord) {
    super(genericRecord);
  }
}

