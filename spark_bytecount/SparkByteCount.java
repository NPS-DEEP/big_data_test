// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

// maybe not
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf

public final class SparkByteCount {

  // ************************************************************
  // SplitInfoWritable containing the split path, start, and length
  // A Writable containing split metadata available for diagnostics.
  // SplitInfoWritable can be used as a key, so it must implement
  // WritableComparable, and not just Writable.
  // ************************************************************
  static class SplitInfoWritable implements
                 org.apache.hadoop.io.WritableComparable<SplitInfoWritable> {

    private String path;
    private long start;
    private long length;

    public SplitInfoWritable(String _path, long _start, long _length) {
      path = _path;
      start = _start;
      length = _length;
    }

    @Override
    public void write(java.io.DataOutput out) throws IOException {
      out.writeUTF(path);
      out.writeLong(start);
      out.writeLong(length);
    }

    @Override
    public void readFields(java.io.DataInput in) throws IOException {
      path = in.readUTF();
      start = in.readLong();
      length = in.readLong();
    }

    @Override
    public int hashCode() {
      return (path.hashCode() * 163 + (int)start) * 163 + (int)length;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SplitInfoWritable) {
        SplitInfoWritable s = (SplitInfoWritable)o;
        return path.equals(s.path) && start == s.start && length == s.length;
      }
      return false;
    }

    @Override
    public String toString() {
      return "path: " + path + ", start: " + start + ", length: " + length;
    }

    @Override
    public int compareTo(SplitInfoWritable s) {
      int cmp = path.compareTo(s.path);
      if (cmp != 0) {
        return cmp;
      }
      if (start > s.start) {
        return 1;
      }
      if (start < s.start) {
        return -1;
      }
      if (length > s.length) {
        return 1;
      }
      if (length < s.length) {
        return -1;
      }
      return 0;
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
    private long more;
    private final long maxStep = 131072; // 2^17=128KiB


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
      more = fileSplit.getLength();

      // set key so diagnostics can have metadata about the split
      currentKey = new SplitInfoWritable(
                                  fileSplit.getPath().toString(), // path
                                  fileSplit.getStart(),           // start
                                  fileSplit.getLength());         // length
//System.out.println("SplitFileRecordReader.initialize: " + currentKey);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

      // done when no more bytes to read in split
      if (more == 0) {
        return false;
      }

      // key
      // currentKey is aready set

      // value
      long count = (more > maxStep) ? maxStep : more;

      byte[] contents = new byte[(int)count];
      org.apache.hadoop.io.IOUtils.readFully(in, contents, 0, (int)count);
      currentValue.set(contents, 0, contents.length);

      // track how many more bytes are available in the split
      more -= count;
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
      return more / (float)currentKey.length;
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
        b.append(i);
        b.append(" ");
        b.append(byteHistogram[i]);
        b.append("\n");
        total += byteHistogram[i];
      }
      b.append("total: ");
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
  // Main
  // ************************************************************

  public static void main(String[] args) {

    if (args.length < 2) {
      System.err.println("Usage: SparkByteCount <input path> <output>");
      System.exit(1);
    }

    org.apache.spark.SparkConf configuration = new org.apache.spark.SparkConf();
    org.apache.spark.api.java.JavaSparkContext sc = new JavaSparkContext(
                            "local", "Spark Byte Count App", configuration);

/*
public <K,V,F extends org.apache.hadoop.mapreduce.InputFormat<K,V>> JavaPairRDD<K,V> newAPIHadoopRDD(org.apache.hadoop.conf.Configuration conf,
                                                                                            Class<F> fClass,
                                                                                            Class<K> kClass,
                                                                                            Class<V> vClass)

Get an RDD for a given Hadoop file with an arbitrary new API InputFormat and extra configuration options to pass to the input format.

Parameters:
    conf - Configuration for setting up the dataset. Note: This will be put into a Broadcast. Therefore if you plan to reuse this conf to create multiple RDDs, you need to make sure you won't modify the conf. A safe approach is always creating a new conf for a new RDD.
    fClass - Class of the InputFormat
    kClass - Class of the keys
    vClass - Class of the values

    '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each record, directly caching the returned RDD will create many references to the same object. If you plan to directly cache Hadoop writable objects, you should first copy them using a map function.
Returns:
    (undocumented)
*/

/*
Class JavaPairRDD<K,V>
*/

// from ByteCount using MapReduce:
  // ************************************************************
  // K1 = SplitInfoWritable 
  // V1 = org.apache.hadoop.io.BytesWritable
  // K2 = org.apache.hadoop.io.NullWritable
  // V2 = ByteHistogramWritable
  // K3 = org.apache.hadoop.io.NullWritable
  // V3 = ByteHistogramWritable
  // ************************************************************

    // get the input RDD
    org.apache.spark.rdd<scala.Tuple2<String,
                         org.apache.spark.input.PortableDataStream>>
               rddSplits = sc.binaryFiles(args[0]);

    rddSplits.map(






    org.apache.spark.api.java.JavaPairRDD<SplitInfoWritable,
                                          org.apache.hadoop.io.BytesWritable>
                  inputRDD = sc.newAPIHadoopRDD(
                       configuration, SplitFileInputFormat.class,
                       ByteHistogramWritable.class,
                       org.apache.hadoop.io.BytesWritable);


    org.apache.spark.api.java.JavaRDD<ByteHistogramWritable>
              histograms = inputRDD.map(







/*
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
      byte[] contents = value.copyBytes();
      for (int i = 0; i< contents.length; i++) {
        ++byteHistogramWritable.byteHistogram[(contents[i]&0xff)];
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
  // K1 = SplitInfoWritable 
  // V1 = org.apache.hadoop.io.BytesWritable
  // K2 = org.apache.hadoop.io.NullWritable
  // V2 = ByteHistogramWritable
  // K3 = org.apache.hadoop.io.NullWritable
  // V3 = ByteHistogramWritable
  // ************************************************************
  public int run(String[] args) throws Exception {

    // p. 26
    // https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
    // http://stackoverflow.com/questions/8603788/hadoop-jobconf-class-is-deprecated-need-updated-example

    // logger
//zz    org.apache.log4j.BasicConfigurator.configure();

    // check usage
    if (args.length != 2) {
      System.err.println("Usage: ByteCount <input path> <output path>");
      System.exit(-1);
    }

    // configuration processed by ToolRunner
    org.apache.hadoop.conf.Configuration configuration = this.getConf();

    // create job
    org.apache.hadoop.mapreduce.Job job =
              org.apache.hadoop.mapreduce.Job.getInstance(configuration,
                      "Byte Count app");
    job.setJarByClass(ByteCount.class);

    // p. 215, p. 212
    job.setInputFormatClass(SplitFileInputFormat.class);
    job.setMapperClass(SplitMapper.class);
    job.setMapOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
    job.setMapOutputValueClass(ByteHistogramWritable.class);
    // partitioner class, not set
    job.setNumReduceTasks(1);
    job.setReducerClass(SplitReducer.class);
    job.setOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
    job.setOutputValueClass(ByteHistogramWritable.class);

    // set input to all files recursively under <input path>
    // http://stackoverflow.com/questions/11342400/how-to-list-all-files-in-a-directory-and-its-subdirectories-in-hadoop-hdfs
    org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(
                                                                   args[0]);
    org.apache.hadoop.fs.FileSystem fileSystem =
                         org.apache.hadoop.fs.FileSystem.get(configuration);
    org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus>
              fileStatusListIterator = fileSystem.listFiles(inputPath, true);
    while (fileStatusListIterator.hasNext()) {
      org.apache.hadoop.fs.LocatedFileStatus locatedFileStatus =
                                               fileStatusListIterator.next();
      org.apache.hadoop.fs.Path path = locatedFileStatus.getPath();

      // skip some files
      if (path.toString().indexOf("/output/") != -1) {
        continue;
      }
      System.out.println("adding path " + path.toString());

      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
                                                locatedFileStatus.getPath());
    }
//    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
//                                   new org.apache.hadoop.fs.Path(
//                                   "Fedora-Xfce-Live-x86_64-24-1.2.iso"));
//    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,
//                                   new org.apache.hadoop.fs.Path(
//                                   "smallfile"));

    // set output to path <output path>
//    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,
//                                     new org.apache.hadoop.fs.Path(args[1]));
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,
                                   new org.apache.hadoop.fs.Path(
                                   "byte_count_output"));

    job.submit();
    return job.waitForCompletion(true) == true ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    org.apache.hadoop.conf.Configuration configuration =
                                new org.apache.hadoop.conf.Configuration();
    String[] remainingArgs = new org.apache.hadoop.util.GenericOptionsParser(
                                     configuration, args).getRemainingArgs();

    int status = org.apache.hadoop.util.ToolRunner.run(
          new org.apache.hadoop.conf.Configuration(), new ByteCount(), args);
    System.exit(status);
  }
*/

