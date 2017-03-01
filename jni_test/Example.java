public final class Example {

  public static void main(String[] args) {
    System.load("example.so");
    System.out.println("main.a");
    final byte[] data = "hi\0jk".getBytes();
    example.binaryChar1(data);
    System.out.println("main.b");
  }
}
