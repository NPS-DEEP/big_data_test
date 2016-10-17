# https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

INPUT = input
#INPUT = /user/tjandrze/DEEP/input
#INPUT = /user/tjandrze/DEEP/sample/input

OUTPUT = byte_count_output

# compile and make jar
ByteCount.jar:
	rm -rf $(OUTPUT)
	hdfs dfs -rm -f -r $(OUTPUT)
	hadoop com.sun.tools.javac.Main ByteCount.java \
                      -Xlint:deprecation -Xlint:unchecked
	jar cf bytecount.jar ByteCount*.class
	hadoop jar bytecount.jar ByteCount $(INPUT) $(OUTPUT)
	hdfs dfs -get $(OUTPUT) $(OUTPUT)

clean:
	rm -f *.class *.jar

