#include <iostream>
#include <exception>

int main(int /* argc */, char** /* argv */) {
  try {
    std::cout << "hello world\n";
    return 0;
  } catch (std::exception& e) {
    std::cerr << "Standard exception. What: " << e.what() << std::endl;
    return 10;
  } catch (...) {
    std::cerr << "Unknown exception" << std::endl;
    return 11;
  }
}
