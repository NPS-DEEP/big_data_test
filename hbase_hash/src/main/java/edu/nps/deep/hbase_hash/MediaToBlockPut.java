package edu.nps.deep.hbase_hash;

import java.util.Iterator;
import org.apache.spark.api.java.function.Function;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public final class MediaToBlockPut implements Function<HashRecord, Put> {
  private static final long serialVersionUID = 1L;

  private static String getKeyPrefix(String path) {
    long offset = Long.parseLong(path);
    return Long.toString(offset/1000000000);
  }

  public Put call(HashRecord hashRecord) throws Exception {

    // create Put object for this feature, key=media hexdigest + offset/1T
    Put put = new Put(Bytes.toBytes(hashRecord.mediaHexdigest +
                                    getKeyPrefix(hashRecord.path)));

    // add the column and value for this feature
    // using tuple (column family="f",
    //              column qualifier=filename+"\t"+offset,
    //              cell value = context, e.g. "...a@b.com..."
    put.addColumn(Bytes.toBytes("f"),        // column family
                  Bytes.toBytes(hashRecord.blockHexdigest +
                                "," + hashRecord.path),
                  Bytes.toBytes(""));

    return put;
  }
}

