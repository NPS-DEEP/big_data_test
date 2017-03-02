// ref 25.8.5 from http://swig.org/Doc3.0/SWIGDocumentation.html#Java_module_packages_classes
#include <cstdlib>
#include <cstdio>
#include "example.h"

void binaryChar1(const char data[], size_t len) {
  printf("len: %d data: ", len);
  for (size_t i=0; i<len; ++i)
    printf("%x ", data[i]);
  printf("\n");
}

