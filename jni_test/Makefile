JAVA_SRC = edu/nps/deep/be_swig
JAVA_PACKAGE = edu.nps.deep.be_swig
INCLUDE = -I/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.79-2.5.5.0.fc20.x86_64/include -I/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.79-2.5.5.0.fc20.x86_64/include/linux

# build the shared library then main, which requires the shared library
all: libexample.so Example.class

libexample.so: example_wrap.o example.o
	ld -G example_wrap.o example.o -o libexample.so

example.o: example.cpp
	gcc -O2 -fno-strict-aliasing -fPIC -c example.cpp

example_wrap.o: example_wrap.cpp
	gcc -O2 -fno-strict-aliasing -fPIC -c example_wrap.cpp $(INCLUDE)

example_wrap.cpp: example.cpp example.i
	swig -package $(JAVA_PACKAGE) -outdir $(JAVA_SRC) -c++ -java -o example_wrap.cpp example.i
	javac $(JAVA_SRC)/*.java

Example.class: Example.java
	javac Example.java

clean:
	rm -f example_wrap.cpp $(JAVA_SRC)/example.java $(JAVA_SRC)/exampleJNI.java $(JAVA_SRC)/*.class *.class *.o *.so

run:
	java Example
