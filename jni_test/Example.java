public final class Example {
  static {
//    try {
//      System.loadLibrary("example");
      System.load("/home/bdallen/big_data_test/jni_test/libexample.so");
/*
    } catch (UnsatisfiedLinkError e) {
      System.err.println("failure in loadLibrary:\n" + e);
      System.exit(1);
    }
*/
  }

  public static void main(String[] args) {
    System.out.println("main.a");
    final byte[] data = "hi\0jk".getBytes();
    edu.nps.deep.be_swig.example.binaryChar1(data);
    System.out.println("main.b");
  }
}
