package edu.nps.deep.hbase_hash;

/**
 * One extracted hash record consisting of media hexdigest,
 * block hexdigest, and path.
 *
 * Although hashes could hold binary content, we use hexcodes for uniformity.
 * Note that the key portion of HBase records cannot be binary.
 */
public final class HashRecord {

  public final String mediaHexdigest;
  public final String blockHexdigest;
  public final String path;

  public HashRecord(String mediaHexdigest, String blockHexdigest,
                    String path) {
    this.mediaHexdigest = mediaHexdigest;
    this.blockHexdigest = blockHexdigest;
    this.path = path;
  }

  public String toString() {
    return "'" + mediaHexdigest + "', '" + blockHexdigest + "', '" + path + "'";
  }
}

