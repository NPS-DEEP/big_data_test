
# compile and make jar
ByteCount.jar:
	rm -rf byte_count_output
	hdfs dfs -rm -f -r byte_count_output
	hadoop com.sun.tools.javac.Main ByteCount.java \
                      -Xlint:deprecation -Xlint:unchecked
	jar cf bytecount.jar ByteCount*.class
	hadoop jar bytecount.jar ByteCount . byte_count_output
	hdfs dfs -get byte_count_output byte_count_output

clean:
	rm -f *.class *.jar

