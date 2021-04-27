#include "cas/binary_key.hpp"

#include "cas/util.hpp"
#include <cstring>
#include <iostream>


void cas::BinaryKey::Dump() const {
  if (data_ == nullptr) {
    std::cout << "<nullptr>\n";
  } else {
    printf("Path  (%2d): ", LenPath()); // NOLINT
    cas::util::DumpHexValues(Path(), LenPath());
    printf("\nValue (%2d): ", LenValue()); // NOLINT
    cas::util::DumpHexValues(Value(), LenValue());
    std::cout << "\nRef:        " <<  ToString(Ref()) << "\n";
  }
}
