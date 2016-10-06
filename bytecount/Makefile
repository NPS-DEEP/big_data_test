# https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

# compile and make jar
ByteCount.jar:
	hadoop com.sun.tools.javac.Main ByteCount.java \
                      -Xlint:deprecation -Xlint:unchecked
	jar cf bytecount.jar ByteCount*.class

