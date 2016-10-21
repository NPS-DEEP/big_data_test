// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.spark_byte_count;

// maybe not
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public final class SparkByteCount {

  // ************************************************************
  // ByteHistogram containing a histogram distribution of bytes.
  // ************************************************************
  static class ByteHistogram implements java.io.Serializable {
    public long[] histogram = new long[256];

    public ByteHistogram() {
      histogram = new long[256];
    }

    public ByteHistogram(PortableDataStream portableIn) {
      histogram = new long[256];

      long bytesRead = 0;
      final int stepSize = 131072; // 2^17=128KiB
      byte[] bytes = new byte[stepSize];

      java.io.DataInputStream in = portableIn.open();
      while (true) {
        int size;
        try {
          size = in.read(bytes);
        } catch (java.io.IOException e) {
          System.err.println("Error in ByteHistogram read: " + e);
          break;
        }
        if (size == -1) {
          // at EOF
          break;
        }

        for (int i = 0; i< size; i++) {
          ++histogram[(bytes[i]&0xff)];
        }
        bytesRead += size;
      }
      System.out.println("ByteHistogram getPath: " + portableIn.getPath()
                         + ", bytes read: " + bytesRead);
    }

    public void add(ByteHistogram other) {
      for (int i = 0; i< histogram.length; i++) {
        histogram[i] += other.histogram[i];
      }
    }

    public String toString() {
      StringBuilder b = new StringBuilder();
      long total = 0;
      for (int i=0; i<256; i++) {
        b.append(i);
        b.append(" ");
        b.append(histogram[i]);
        b.append("\n");
        total += histogram[i];
      }
      b.append("total: ");
      b.append(total);
      return b.toString();
    }
  }

  // ************************************************************
  // Main
  // ************************************************************

  public static void main(String[] args) {

    if (args.length != 1) {
      System.err.println("Usage: SparkByteCount <input path>");
      System.exit(1);
    }

    SparkConf configuration = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(
                            "local", "Spark Byte Count App", configuration);

    // get the input pair RDD
    JavaPairRDD<String, PortableDataStream> pairRDDSplits = 
                                             sc.binaryFiles(args[0]);

    // map to get individual histograms from each split
    JavaRDD<ByteHistogram> histogramRDD = pairRDDSplits.map(
            new Function <Tuple2<String, PortableDataStream>,
            ByteHistogram>() {
      @Override
      public ByteHistogram call(Tuple2<String, PortableDataStream> pair)
                                                           throws Exception {
        return new ByteHistogram(pair._2());
      }
    });

    // join individual histograms to get histogram total
    ByteHistogram histogramTotal = histogramRDD.reduce(
            new Function2< ByteHistogram, ByteHistogram, ByteHistogram>() {
      @Override
      public ByteHistogram call(ByteHistogram v1, ByteHistogram v2) {
        ByteHistogram v3 = new ByteHistogram();
        v3.add(v1);
        v3.add(v2);
        return v3;
      }
    });

    // show histogram total
    System.out.println("Histogram total:\n" + histogramTotal);
  }
}

