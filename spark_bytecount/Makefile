INPUT = input
#INPUT = /user/tjandrze/DEEP/input
#INPUT = /user/tjandrze/DEEP/sample/input

OUTPUT = spark_byte_count_output

# compile and make jar
SparkByteCount.jar:
	rm -rf $(OUTPUT)
	hdfs dfs -rm -f -r $(OUTPUT)
	hadoop com.sun.tools.javac.Main SparkByteCount.java \
                      -Xlint:deprecation -Xlint:unchecked
	jar cf sparkbytecount.jar SparkByteCount*.class
	hadoop jar sparkbytecount.jar SparkByteCount $(INPUT) $(OUTPUT)
	hdfs dfs -get $(OUTPUT) $(OUTPUT)

clean:
	rm -f *.class *.jar

