INCLUDE = -i/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.79-2.5.5.0.fc20.x86_64/include -i/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.79-2.5.5.0.fc20.x86_64/include/linux

# build Java and the shared library
all: Example.class libexample.so

Example.class: Example.java
	javac Example.java

libexample.so: example_wrap.o example.o
	ld -G example_wrap.o example.o -o libexample.so

example.o: example.cpp
	gcc -o2 -fno-strict-aliasing -fPIC -c example.cpp

example_wrap.o: example_wrap.cpp
	gcc -o2 -fno-strict-aliasing -fPIC -c example_wrap.cpp $(INCLUDE)

example_wrap.c: example.cpp example.i
	swig -c++ -java example.i

