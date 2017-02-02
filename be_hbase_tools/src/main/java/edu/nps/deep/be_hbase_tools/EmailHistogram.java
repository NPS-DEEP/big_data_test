// https://www.tutorialspoint.com/hbase/hbase_read_data.htm
// http://stackoverflow.com/questions/17939084/get-all-values-of-all-rows-in-hbase-using-java

package edu.nps.deep.be_hbase_tools;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.client.Get;

public final class EmailHistogram{

  public static void main(String[] args) throws IOException, Exception {

    // HBase configuration to work with
    Configuration config = HBaseConfiguration.create();

    // the table containing the RDC features
    HTable table = new HTable(config, "rdc_feature_table");

    // a scan object
    Scan scan = new Scan();
    scan.setCaching(1000);
    scan.addFamily(Bytes.toBytes("f"));

    // a scanner
    ResultScanner scanner = table.getScanner(scan);
    for (Result result = scanner.next(); result != null;
                                         result = scanner.next()) {
      for(KeyValue keyValue : result.list()) {
        System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()));
      }
    }
  }
}
