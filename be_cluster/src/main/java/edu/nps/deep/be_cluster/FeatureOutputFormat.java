// Adapted from org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.java

package edu.nps.deep.be_cluster;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

public class FeatureOutputFormat extends FileOutputFormat<Long, Features> {

  protected static class FeatureRecordWriter
                                 extends RecordWriter<Long, Features> {

    protected DataOutputStream out;

    public FeatureRecordWriter(DataOutputStream out) {
      this.out = out;
    }

    public synchronized void write(Long key, Features value)
                                 throws IOException {

      // do not write key

      // write each feature
      Iterator<Feature> it = value.iterator();
      while (it.hasNext()) {
        Feature feature = it.next();
        out.writeUTF(feature.forensicPath + "\t" + feature.featureBytes);
      }
    }

    public synchronized 
    void close(TaskAttemptContext context) throws IOException {
      out.close();
    }
  }

  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext job
                         ) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    String extension = "";
    Path file = getDefaultWorkFile(job, extension);
    FileSystem fs = file.getFileSystem(conf);

    FSDataOutputStream fileOut = fs.create(file, false);
    return new FeatureRecordWriter(fileOut);
  }
}

