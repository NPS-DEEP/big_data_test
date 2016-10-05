// based loosely on https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
// and Hadoop 4'th ed. p. 24, p. 229.

public class ByteCount extends org.apache.hadoop.conf.Configured
                       implements org.apache.hadoop.conf.Tool {

  // ************************************************************
  // SplitFileReacordReader reads the requested split.
  // MapReduce should try to dispatch the task to run on the node containing
  // this split.
  // ref. https://github.com/apache/mahout/blob/master/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java
  // ************************************************************
  static SplitFileRecordReader extends RecordReader<
                           SplitInfoWritable,
                           org.apache.hadoop.io.BytesWritable> {

    private SplitInfoWritable currentKey;
    private org.apache.hadoop.io.BytesWritable currentValue =
                                 new org.apache.hadoop.io.BytesWritable();
    private org.apache.hadoop.fs.FSDataInputStream in;
    private boolean isDone = false;

    @override
    public void initialize(
                 org.apache.hadoop.mapred.InputSplit inputSplit,
                 org.apache.hadoop.mapred.TaskAttemptContext context)
                        throws IOException, InterruptedException {

      // open the input file
      final long start = inputSplit.getStart();
      final long end = inputSplit.getEnd();
      final org.apache.hadoop.fs.Path path = inputSplit.getPath();
      final org.apache.hadoop.fs.FileSystem fileSystem =
                   path.getFileSystem(configuration
zzzz

                     inputSplit.getFileSystem();

      org.apache.hadoop.mapred.FileSplit fileSplit =
                       (org.apache.hadoop.mapred.FileSplit)inputSplit;




    }

    @override
    public boolean nextKeyValue() throws IOException, InterruptedException {

      // key
      key = new SplitInfoWritable(fileSplit.getPath().toString(), // path
                                  fileSplit.getStart(),           // start
                                  fileSplit.getLength());         // length

      // value
      byte[] contents = new byte[(int)fileSplit.getLength()];
      Path file = fileSplit.getPath();
      org.apache.hadoop.fs.FileSystem fs = file.getFileSystem(configuration);
      org.apache.hadoop.fs.FSDataInputStream in = null;
      try {
        in = fs.open(file);
        org.apache.commons.io.IOUtils.readFully(
                                          in, contents, 0, contents.length);
        value.set(contents, 0, contents.length);
      } finally {
        org.apache.commons.io.IOUtils.closeStream(in);
      }
      isDone = true;
      return true;
    }

    @override
    public SplitInfoWritable getCurrentKey()
                                  throws IOException, InterruptedException {
      return key;
    }

    @override
    public org.apache.hadoop.io.BytesWritable getCurrentValue()
                                  throws IOException, InterruptedException {
      return value;
    }

    @override
    public float getProgress() throws IOException {
      return isDone ? 1.0f : 0.0f;
    }

    @override
    public float close() throws IOException {
      // no resources to close
    }
  }

  // ************************************************************
  // SplitFileInputFormat provides createRecordReader which returns
  // SplitFileRecordReader which returns one split given mapred.FileSplit
  // information.
  // ************************************************************
  static class SplitFileInputFormat
        extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<
                         SplitInfoWritable,
                         org.apache.hadoop.io.BytesWritable> {

    // createRecordReader returns SplitFileRecordReader
    @override
    public org.apache.hadoop.mapred.RecordReader<
                         SplitInfoWritable,
                         org.apache.hadoop.io.BytesWritable>
           createRecordReader(
                 org.apache.hadoop.mapred.InputSplit split,
                 org.apache.hadoop.mapred.TaskAttemptContext context)
                       throws IOException, InterruptedException {

      SplitFileRecordReader reader = new SplitFileRecordReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  // ************************************************************
  // SplitInfoWritable containing the split path, start, and length
  // ************************************************************
  static class SplitInfoWritable implements org.apache.hadoop.io.Writable {

    private String path;
    private int start;
    private int length;

    public SplitInfoWritable(String _path, int _start, int _length) {
      path = _path;
      start = _start;
      length = _length;
    }

    public void write(java.io.DataOutput out) throws IOException {
      out.WriteString(path);
      out.WriteInt(start);
      out.WriteInt(length);
    }

    public void readFields(java.io.DataInput in) throws IOException {
      path = in.readString();
      start = in.readInt();
      length = in.readInt();
    }

    public static SplitInfoWritable read(java.io.DataInput in)
                                                     throws IOException {

      SplitInfoWritable w = new SplitInfoWritable();
      w.readFields(in);
      return w;
    }
  }

  // ************************************************************
  // SplitMapper
  // ref. p. 230.
  // ref. https://hadoop.apache.org/docs/r2.6.2/api/org/apache/hadoop/mapreduce/Mapper.html
  // ************************************************************
  static class SplitMapper extends org.apache.hadoop.mapreduce.Mapper<
                       SplitInfoWritable,
                       org.apache.hadoop.io.BytesWritable,
                       SplitInfoWritable,
                       org.apache.hadoop.io.Text> {

    private SplitInfoWritable splitInfoWritable;

    @override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
                                   throws IOException, InterruptedException {
      Path path = ((org.apache.hadoop.mapreduce.lib.input.FileSplit)
                                                            split).getPath();
      splitInfoWritable = new SplitInfoWritable(path.toString(),
                                                split.getStart(),
                                                split.getLength());
    }

    @override
    protected void map(SplitInfoWritable key, 




  // ************************************************************
  // Main
  // ************************************************************
  @override
  public int run(String[] args) throws Exception {

    // p. 26
    org.apache.hadoop.mapreduce.Job job = new
                                          org.apache.hadoop.mapreduce.Job();

    job.setJarByClass(ByteCount.class);
    job.setJobName("Byte Count app, prelude to RDC byte count histogram");

    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(
                              new path("Fedora-Xfce-Live-x86_64-24-1.2.iso"));

    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setOutputPath(
                              new path("byte_count_output_1"));

    job.setMapperClass(
    

