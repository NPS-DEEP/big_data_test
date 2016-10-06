// based loosely on https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
// and Hadoop 4'th ed. p. 24, p. 229.

import java.io.IOException;

//public class ByteCount extends org.apache.hadoop.conf.Configured
//                       implements org.apache.hadoop.util.Tool {
public class ByteCount {

  // ************************************************************
  // SplitInfoWritable containing the split path, start, and length
  // A Writable containing split metadata available for diagnostics.
  // ************************************************************
  static class SplitInfoWritable implements org.apache.hadoop.io.Writable {

    private String path;
    private long start;
    private long length;

    public SplitInfoWritable(String _path, long _start, long _length) {
      path = _path;
      start = _start;
      length = _length;
    }

    public void write(java.io.DataOutput out) throws IOException {
      out.writeUTF(path);
      out.writeLong(start);
      out.writeLong(length);
    }

    public void readFields(java.io.DataInput in) throws IOException {
      path = in.readUTF();
      start = in.readLong();
      length = in.readLong();
    }

    public static SplitInfoWritable read(java.io.DataInput in)
                                                     throws IOException {

      SplitInfoWritable w = new SplitInfoWritable("",0,0);
      w.readFields(in);
      return w;
    }
  }

  // ************************************************************
  // SplitFileReacordReader reads the requested split.
  // MapReduce should try to dispatch the task to run on the node containing
  // this split.
  // ref. https://github.com/apache/mahout/blob/master/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java
  // ************************************************************
  static class SplitFileRecordReader
                            extends org.apache.hadoop.mapreduce.RecordReader<
                                 SplitInfoWritable,
                                 org.apache.hadoop.io.BytesWritable> {

    private org.apache.hadoop.fs.FSDataInputStream in;
    private SplitInfoWritable currentKey;
    private org.apache.hadoop.io.BytesWritable currentValue =
                                 new org.apache.hadoop.io.BytesWritable();
    private boolean isDone = false;


    @Override
    public void initialize(
                 org.apache.hadoop.mapreduce.InputSplit inputSplit,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                        throws IOException, InterruptedException {

      // get InputSplit in terms of FileSplit
      final org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit =
                 (org.apache.hadoop.mapreduce.lib.input.FileSplit)inputSplit;

      // open the input file and seek to the split
      final long start = fileSplit.getStart();
      final long length = fileSplit.getLength();
      final org.apache.hadoop.fs.Path path = fileSplit.getPath();
      final org.apache.hadoop.conf.Configuration configuration =
                                              context.getConfiguration();
      final org.apache.hadoop.fs.FileSystem fileSystem =
                                       path.getFileSystem(configuration);
      in = fileSystem.open(path);
      in.seek(start);

      // set key so diagnostics can have metadata about the split
      currentKey = new SplitInfoWritable(
                                  fileSplit.getPath().toString(), // path
                                  fileSplit.getStart(),           // start
                                  fileSplit.getLength());         // length

      // although the value can be read during initialize(), save it
      // for nextKeyValue().
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

      // Unlike most readers which return parts of the split as keyed
      // structures, SplitFileRecordReader reads and returns the entire
      // split in one giant BytesWritable value.
      if (isDone == true) {
        return false;
      }

      // key
      // currentKey is aready set

      // value
      byte[] contents = new byte[(int)currentKey.length];
      org.apache.hadoop.io.IOUtils.readFully(in, contents, 0, contents.length);
      currentValue.set(contents, 0, contents.length);

      // done
      isDone = true;
      return true;
    }

    @Override
    public SplitInfoWritable getCurrentKey()
                                  throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public org.apache.hadoop.io.BytesWritable getCurrentValue()
                                  throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
      return isDone ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
      if (in != null) {
        org.apache.hadoop.io.IOUtils.closeStream(in);
      }
    }
  }

  // ************************************************************
  // SplitFileInputFormat provides createRecordReader which returns
  // SplitFileRecordReader for one split.
  // ************************************************************
  static class SplitFileInputFormat
        extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<
                         SplitInfoWritable,
                         org.apache.hadoop.io.BytesWritable> {

    // createRecordReader returns SplitFileRecordReader
    @Override
    public org.apache.hadoop.mapreduce.RecordReader<
                         SplitInfoWritable,
                         org.apache.hadoop.io.BytesWritable>
           createRecordReader(
                 org.apache.hadoop.mapreduce.InputSplit split,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                       throws IOException, InterruptedException {

      SplitFileRecordReader reader = new SplitFileRecordReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  // ************************************************************
  // ByteHistogramWritable containing a histogram distribution of bytes.
  // ************************************************************
  static class ByteHistogramWritable implements org.apache.hadoop.io.Writable {

    public long[] byteHistogram = new long[256];

    public ByteHistogramWritable() {
      byteHistogram = new long[256];
    }

    public void write(java.io.DataOutput out) throws IOException {
      for (int i=0; i<256; i++) {
        out.writeLong(byteHistogram[i]);
      }
    }

    public void readFields(java.io.DataInput in) throws IOException {
      for (int i=0; i<256; i++) {
        byteHistogram[i] = in.readLong();
      }
    }

    public void add(ByteHistogramWritable other) {
      for (int i = 0; i< byteHistogram.length; i++) {
        byteHistogram[i] += other.byteHistogram[i];
      }
    }

    public String toString() {
      StringBuilder b = new StringBuilder();
      long total = 0;
      for (int i=0; i<256; i++) {
        b.append(byteHistogram[i]);
        b.append(" ");
        total += byteHistogram[i];
      }
      b.append(total);
      return b.toString();
    }

    public static ByteHistogramWritable read(java.io.DataInput in)
                                                     throws IOException {

      ByteHistogramWritable w = new ByteHistogramWritable();
      w.readFields(in);
      return w;
    }
  }

  // ************************************************************
  // SplitMapper
  // ref. p. 230.
  // ref. https://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  // Currently: take bytes from reader and count them into the bytes histogram.
  // ************************************************************
  public static class SplitMapper extends org.apache.hadoop.mapreduce.Mapper<
                       SplitInfoWritable,
                       org.apache.hadoop.io.BytesWritable,
                       org.apache.hadoop.io.NullWritable,
                       ByteHistogramWritable> {

    @Override
    public void map(SplitInfoWritable key,
                    org.apache.hadoop.io.BytesWritable value,
                    Context context)
                                throws IOException, InterruptedException {

      // populate a new ByteHistogramWriable from the bytes from the split
      ByteHistogramWritable byteHistogramWritable = new ByteHistogramWritable();
      byte[] contents = value.getBytes();
      for (int i = 0; i< contents.length; i++) {
        ++byteHistogramWritable.byteHistogram[i];
      }

      // write the new key, value tuple
      context.write(org.apache.hadoop.io.NullWritable.get(),
                    byteHistogramWritable);
    }
  }

  // ************************************************************
  // SplitReducer
  // ref. p. 25.
  //
  // Currently: take bytes from reader and count them into the bytes histogram.
  // ************************************************************
  public static class SplitReducer extends org.apache.hadoop.mapreduce.Reducer<
                       org.apache.hadoop.io.NullWritable,
                       ByteHistogramWritable,
                       org.apache.hadoop.io.NullWritable,
                       ByteHistogramWritable> {

    public void reduce(org.apache.hadoop.io.NullWritable key,
                       Iterable<ByteHistogramWritable> values,
                       Context context)
                                throws IOException, InterruptedException {

      // populate a new ByteHistogramWriable from the bytes from the split
      ByteHistogramWritable byteHistogramWritable = new ByteHistogramWritable();

      // reduce all values as they come back to this single reducer
      for (ByteHistogramWritable value : values) {

        // add the value array to the histogram
        byteHistogramWritable.add(value);
      }

      // write the new key, value tuple
      context.write(org.apache.hadoop.io.NullWritable.get(),
                    byteHistogramWritable);
    }
  }

  // ************************************************************
  // ByteCount Main
  // ************************************************************
  public static void main(String[] args) throws Exception {
    // p. 26
    // https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

    org.apache.hadoop.conf.Configuration configuration =
                                  new org.apache.hadoop.conf.Configuration();
    org.apache.hadoop.mapreduce.Job job =
              org.apache.hadoop.mapreduce.Job.getInstance(configuration,
                      "Byte Count app, prelude to RDC byte count histogram");
    job.setJarByClass(ByteCount.class);

    job.setMapperClass(SplitMapper.class);
//?    job.setCombinerClass(SplitReducer.class);
    job.setReducerClass(SplitReducer.class);

    job.setOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
    job.setOutputValueClass(ByteHistogramWritable.class);

    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
                                   new org.apache.hadoop.fs.Path(
                                   "Fedora-Xfce-Live-x86_64-24-1.2.iso"));
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,
                                   new org.apache.hadoop.fs.Path(
                                   "byte_count_output_1"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

