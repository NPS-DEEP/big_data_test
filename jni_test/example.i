
%{
#include "example.hpp"
%}

%apply (char *STRING, size_t LENGTH) { ( const char data[], size_t len) }

%feature("autodoc", "1");

%include "example.hpp"

// ref. 25.8.5

