#include "cas/util.hpp"
#include "cas/types.hpp"

#include <cstring>
#include <iostream>
#include <sstream>
#include <regex>
#include <iomanip>

void cas::util::DumpHexValues(const std::vector<std::byte>& buffer) {
  DumpHexValues(buffer, 0, buffer.size());
}

void cas::util::DumpHexValues(const std::vector<std::byte>& buffer, size_t size) {
  DumpHexValues(buffer, 0, size);
}

void cas::util::DumpHexValues(const std::vector<std::byte>& buffer,
    size_t offset, size_t size) {
  DumpHexValues(buffer.data(), offset, size);
}

void cas::util::DumpHexValues(const std::byte* buffer, size_t size) {
  DumpHexValues(buffer, 0, size);
}

void cas::util::DumpHexValues(const std::byte* buffer,
    size_t offset, size_t size) {
  for (size_t i = offset; i < size; ++i) {
    printf("0x%02X", static_cast<unsigned char>(buffer[i])); // NOLINT
    if (i < size-1) {
      printf(" "); // NOLINT
    }
  }
}

std::string cas::util::IndexName(bool reverse_paths) {
  return reverse_paths ? "/data/indexes/index_backward.bin" : "/data/indexes/index_forward.bin";
}

std::string cas::util::CreateLink(std::string revision){
   std::string linkTemplate = "https://archive.softwareheritage.org/browse/revision/REF/#swh-revision-changes";
   return std::regex_replace(linkTemplate, std::regex("REF"), revision);
}

std::string cas::util::IsoToUnix(std::string isoTimestamp){
    // Requires input of form YYYY-MM-TT[T]hh:mm:ss[Z], e.g. 2013-04-14T12:05:32Z)
    int Y, M, T, h, m, s;
    time_t timeSinceEpoch;
    if (!cas::util::isValidIsoTimestamp(isoTimestamp)) {
        throw std::runtime_error{"Timestamp is not in correct ISO format"};
    }
    sscanf(isoTimestamp.c_str(), "%d-%d-%dT%d:%d:%dZ", &Y, &M, &T, &h, &m, &s);
    tm time = {};
    time.tm_year = Y - 1900; // Year since 1900
    time.tm_mon = M - 1;     // 0-11
    time.tm_mday = T;        // 1-31
    time.tm_hour = h;        // 0-23
    time.tm_min = m;         // 0-59
    time.tm_sec = (int)s;    // 0-61 (0-60 in C++11)
    timeSinceEpoch = mktime(&time);
    std::stringstream ss;
    ss << timeSinceEpoch;
    std::string unixTimestamp = ss.str();
    return unixTimestamp;
}

std::string cas::util::UnixToIso(std::string unixTimestamp){
    if (!isUnixTimestampString(unixTimestamp)) {
        throw std::runtime_error{"Not valid Unix timestamp between year 1970 and 2099"};
    }
    time_t timeSinceEpoch = std::stol(unixTimestamp);
    tm *time = gmtime(&timeSinceEpoch);
    auto Y = std::to_string(time->tm_year+1900);
    auto M = ((std::to_string(time->tm_mon+1).length()==1) ? "0" : "") + std::to_string(time->tm_mon+1);
    auto T = ((std::to_string(time->tm_mday).length()==1) ? "0" : "") + std::to_string(time->tm_mday);
    auto h = ((std::to_string(time->tm_hour).length()==1) ? "0" : "") + std::to_string(time->tm_hour);
    auto m = ((std::to_string(time->tm_min).length()==1) ? "0" : "") + std::to_string(time->tm_min);
    auto s = ((std::to_string(time->tm_sec).length()==1) ? "0" : "") + std::to_string(time->tm_sec);
    std::string isoTimestamp = Y + "-" + M + "-" + T + "T" + h + ":" + m + ":" + s + "Z";
    return isoTimestamp;
}

bool cas::util::isUnixTimestampString(std::string unixTimestamp) {
    std::string::const_iterator it = unixTimestamp.begin();
    while (it != unixTimestamp.end() && std::isdigit(*it)) ++it;
    // Check if timestamp between 1970 and 2099
    return !unixTimestamp.empty() && it == unixTimestamp.end() && std::stol(unixTimestamp) >= 0 && std::stol(unixTimestamp) < 4102444800;
}

std::string cas::util::calculateIndexDirection(std::string path) {
    std::string reversedPath = reversePath(path);
    size_t firstForward = path.find_first_of('*');
    size_t firstBackward = reversedPath.find_first_of('*');
    return (firstBackward > firstForward) ? "1" : "0";
}

std::string cas::util::ReplaceString(std::string subject, const std::string& search,
                          const std::string& replace) {
    size_t pos = 0;
    while ((pos = subject.find(search, pos)) != std::string::npos) {
         subject.replace(pos, search.length(), replace);
         pos += replace.length();
    }
    return subject;
}

std::string cas::util::reversePath(std::string path) {
    size_t pos = 0;
    std::string reversed = "/";
    std::string label;
    while ((pos = path.find('/')) != std::string::npos) {
        label = path.substr(0, pos);
        if (label != "") {
        reversed = label + "/" + reversed;
        }
        path.erase(0, pos + 1);
    }
    reversed = path + "/" + reversed;
    reversed = reversed.substr(0, reversed.length()-1);
    return reversed;
}

bool cas::util::isValidPath(std::string path) {
    // non-empty
    if (path.length() == 0) {
        std::cout << "Must be non-empty";
        return false;
    }
    // begins with '/'' (or '\/'), does not end with slash
    if (path.at(0) != '/' && (path.at(0) != '\\' && path.at(1) != '/')) {
        std::cout << "Must start with '/'";
        return false;
    }
    if (path.at(path.length()-1) == '/') {
        std::cout << "Must end without '/'";
        return false;
        }
    // does not contain '//'
    if (path.find("//") !=std::string::npos) {
        std::cout << "Shall not contain '//'";
        return false;
        }
    // does not contain '***'
    if (path.find("***") != std::string::npos) {
        std::cout << "Shall not contain '***'";
        return false;}
    // '**' only if '/**/' or '/**' at the end of path
    int nr_wildcards = 0;
    bool escaped = false;
    bool label_start = false;
    for (char symbol : path) {
        if (!escaped && symbol == '\\') {
            escaped = true;
        } else {
            // if non-slash symbol after '**'
            if (nr_wildcards == 2 && symbol != '/') {
                std::cout << "non-slash symbol after **";
                return false;}
            if (symbol == '/') {
                nr_wildcards = 0;
                label_start = true;
            } else if (symbol == '*' && !escaped) {
                ++nr_wildcards;
            } else {
                nr_wildcards = 0;
                label_start = false;
            }
            // if non-slash symbol before '**'
            if (nr_wildcards == 2 && !label_start) {
                std::cout << "non-slash symbol before **";
                return false;}
            escaped = false;
        }
    }
    return true;
}

bool cas::util::isValidIsoTimestamp(std::string timestamp) {
    const std::regex isoRegex("^(20[0-9]{2}|19[7-9][0-9])-(0[1-9]|1[0-2])-(3[0-1]|[0-2][0-9])T(2[0-3]|[0-1][0-9]):([0-5][0-9]):([0-5][0-9])Z$");
    if (!std::regex_match(timestamp, isoRegex)) {
        return false;
    }
    return true;
}

bool cas::util::isValidDirection(std::string direction) {
    if (direction =="0" || direction =="1" || direction =="auto") {
        return true;
    }
    return false;
}



void cas::util::Log(const std::string& msg) {
  std::cout << "[" << cas::util::CurrentIsoTime() << "] " << msg;
}


std::string cas::util::CurrentIsoTime() {
  auto now = std::chrono::system_clock::now();
  auto stime = std::chrono::system_clock::to_time_t(now);
  auto ltime = std::localtime(&stime);
  std::stringstream stream;
  stream << std::put_time(ltime, "%FT%T%z");
  return stream.str();
}
