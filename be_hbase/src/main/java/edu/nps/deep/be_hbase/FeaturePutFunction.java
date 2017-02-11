package edu.nps.deep.be_hbase;

import java.util.Iterator;
import org.apache.spark.api.java.function.Function;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public final class FeaturePutFunction implements Function<Feature, Put> {
  private static final long serialVersionUID = 1L;

  public Put call(Feature feature) throws Exception {

    // create Put object for this feature, email row is, e.g., a@b.com.
    Put put = new Put(Bytes.toBytes(feature.feature));

    // add the column and value for this feature
    // using tuple (column family="f",
    //              column qualifier=filename+"\t"+offset,
    //              cell value = context, e.g. "...a@b.com..."
    put.addColumn(Bytes.toBytes("f"),        // column family
                  Bytes.toBytes(feature.filename + "," + feature.path),
                  Bytes.toBytes(""));

    return put;
  }

/*
  public Put call(Feature feature) throws Exception {

    // create Put object for this feature, email row is tuple,
    // e.g. "email,a@b.com"
    Put put = new Put(Bytes.toBytes("email," + feature.filename +
                                    "," + feature.path));

    // add the column and value for this feature
    // using tuple (column family="f",
    //              column qualifier=filename+"\t"+offset,
    //              cell value = context, e.g. "...a@b.com..."
    put.addColumn(Bytes.toBytes("f"),        // column family
                  Bytes.toBytes(feature.feature),
                  Bytes.toBytes(feature.context));

    return put;
  }
*/
}

