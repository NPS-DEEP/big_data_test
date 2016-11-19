JAVA_SRC = src/main/java/edu/nps/deep/be_cluster
JFLEX_URL = http://jflex.de/release/jflex-1.6.1.zip
JFLEX_JAR = jflex-1.6.1/lib/jflex-1.6.1.jar
all: $(JAVA_SRC)/BinaryLexer.java
	gradle build --info

run:
	gradle run --stacktrace --info --debug

run2:
	gradle run2 --stacktrace --info --debug

run3:
	gradle run3 --stacktrace --info --debug

$(JFLEX_JAR):
	wget $(JFLEX_URL)
	unzip jflex-1.6.1.zip

$(JAVA_SRC)/BinaryLexer.java: $(JAVA_SRC)BinaryLexer.flex $(JFLEX_JAR)
	java -cp $(JFLEX_JAR) -Xmx1000m JFlex.main \
                        -d $(JAVA_SRC) $(JAVA_SRC)/BinaryLexer.flex

