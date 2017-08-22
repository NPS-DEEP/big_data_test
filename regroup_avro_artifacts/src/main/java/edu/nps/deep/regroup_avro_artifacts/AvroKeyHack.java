package edu.nps.deep.regroup_avro_artifacts;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.generic.GenericRecord;

import edu.nps.deep.be_scan.Artifact;

public final class AvroKeyHack extends AvroKey<GenericRecord> {

  AvroKeyHack(GenericRecord genericRecord) {
    super(genericRecord);
  }
}

