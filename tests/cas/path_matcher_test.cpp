#include "test/catch.hpp"
#include "cas/key_encoder.hpp"
#include "cas/path_matcher.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <map>
#include <string>
#include <iterator>


TEST_CASE("Matching a complete path", "[cas::PathMatcher]") {
   auto match_normal = [&](cas::Key<cas::vint64_t> ikey,
         cas::SearchKey<cas::vint64_t> skey) -> bool {

      // encode input pattern
      cas::QueryBuffer input_buffer1;
      cas::QueryBuffer input_buffer2;
      cas::BinaryKey bikey{&input_buffer1.at(0)};
      cas::KeyEncoder<cas::vint64_t>::Encode(ikey, bikey);
      memcpy(&input_buffer2.at(0), bikey.Path(), bikey.LenPath());
      // encode query pattern
      cas::BinarySK bskey = cas::KeyEncoder<cas::vint64_t>::Encode(skey);

      return cas::path_matcher::MatchPath(input_buffer2, bskey.path_,
                                          bikey.LenPath());
   };

   auto match = [&](std::string input_path, std::string pattern) -> bool {
      // build input key with path
      cas::vint64_t value = 0;
      cas::ref_t ref;
      cas::Key<cas::vint64_t> key{input_path, value, ref};
      cas::SearchKey<cas::vint64_t> skey{pattern, value, value};
      return match_normal(key, skey);
   };



   auto match_partial = [&](std::string input_path, std::string pattern) -> cas::path_matcher::PrefixMatch {
      // build input key with path
      cas::vint64_t value = 0;
      cas::ref_t ref;
      cas::Key<cas::vint64_t> ikey{input_path, value, ref};
      cas::SearchKey<cas::vint64_t> skey{pattern, value, value};
      cas::QueryBuffer input_buffer1;
      cas::QueryBuffer input_buffer2;
      cas::BinaryKey bikey{&input_buffer1.at(0)};
      cas::KeyEncoder<cas::vint64_t>::Encode(ikey, bikey);
      memcpy(&input_buffer2.at(0), bikey.Path(), bikey.LenPath());
      // encode query pattern
      cas::BinarySK bskey = cas::KeyEncoder<cas::vint64_t>::Encode(skey);
      cas::path_matcher::State s;
      return cas::path_matcher::MatchPathIncremental(input_buffer2, bskey.path_,
                                                     bikey.LenPath()-1,s);
   };

   SECTION("Filename Cases"){
      std::string pattern = "/foo/file*.py";
      REQUIRE(match("/foo/file2.pyp", pattern) == false);

      REQUIRE(match("/foo/file.py", pattern));
      REQUIRE(match("/foo/file1.py", pattern));
      REQUIRE(match("/foo/file2.py", pattern));
      REQUIRE(match("/foo/2file.py", pattern) == false);
   }

   SECTION("Easy") {
      std::string input =  "/ab/aa/ab/cc/abd";
      REQUIRE(match(input, "/ab/aa/ab/cc/abd"));
      REQUIRE(match(input, "/ab/aa/ac/cc/abd") == false);
      REQUIRE(match(input, "/ab/aa") == false);
   }

   SECTION("Descriptive Cases") {
     std::vector<std::string> inputs = {"/a/c",
                                    "/a/b/c",
                                    "/a/b1/b2/c",
                                    "/a/foo/bar.py",
                                    "/a/foo.py",
                                    "/a/b/foo.py",
                                    "/a"
                                   };


      std::map<std::string, std::vector<bool>> patternResultMap;
      patternResultMap["/a/**/c"]   = {true, true, true, false, false, false, false};
      patternResultMap["/a/*/c"]    = {false, true, false, false, false, false, false};
      patternResultMap["/a/f*.py"]  = {false, false, false, false, true, false, false};
      patternResultMap["/a/*"]      = {true, false, false, false, true, false, false};
      patternResultMap["/a/**"]     = {true, true, true, true, true, true, true};

      std::cout << "Testing descriptive cases" << std::endl;
      std::map<std::string, std::vector<bool>>::iterator it = patternResultMap.begin();
      while (it != patternResultMap.end()) {
         std::string pattern = it->first;
         std::vector<bool> expectedResults = it->second;

         std::cout << "Pattern:" << pattern << std::endl;
         for (size_t i = 0; i< inputs.size(); i++) {
            std::cout << "Input: " << inputs[i] << " Expected result: " << expectedResults[i] << std::endl;
            REQUIRE(match(inputs[i], pattern) == expectedResults[i]);
         }
         it++;
      }
   }

   SECTION("Incomplete Wildcard Axis Match") {
      std::string input =  "/ab/ab";
      REQUIRE(match_partial(input, "/ab/**/abc.py") == cas::path_matcher::PrefixMatch::INCOMPLETE);

   }

   SECTION("Child Wildcard Axis") {
      std::string input =  "/ab/cd/x/a/b";

      REQUIRE(match(input, "/**/ab/*/x/a/b")); // / added at start

      REQUIRE(match(input, "/**/ab/*/x/**")); // / added at start


      REQUIRE(match(input, "/ab/*/x/*/*"));
      REQUIRE(match(input, "/*/*/*/*/*"));


      REQUIRE(match(input, "/*/**/*"));
      REQUIRE(match(input, "/*/*/*/*/*"));

      REQUIRE(match(input, "/*/*/*/*") == false);
      REQUIRE(match(input, "/*/dd/*/*/*") == false);

      input =  "/ab/abc/abc/abc.py";

      // c1 wildcard at begining
      REQUIRE(match(input, "/**/abc/abc.py"));
      // c2: wildcard somewhere in the middle
      REQUIRE(match(input, "/ab/abc/**/abc.py"));
      REQUIRE(match(input, "/ab/**/abc.py"));
      // c3: unnecessary wildcard
      REQUIRE(match(input, "/ab/abc/abc/**/abc.py"));
      REQUIRE(match(input, "/ab/**"));
      //check *
      REQUIRE(match(input, "/ab/abc/*abc/abc.py"));
      REQUIRE(match(input, "/ab/abc/abc*/abc.py"));
      REQUIRE(match(input, "/ab/*/abc/abc.py"));
      REQUIRE(match(input, "/ab/abc/*bc/abc.py"));
      REQUIRE(match(input, "/ab/abc/a*c/abc.py"));
      REQUIRE(match(input, "/ab/abc/abc/*.py"));
      REQUIRE(match(input, "/ab/abc/abc/*"));
      REQUIRE(match(input, "/ab/abc/abc/abc.py/**"));
      input = "a/b/xybtttbab/abc.py";
      REQUIRE(match(input, "a/b/*b/abc.py"));
   }

   SECTION("Descendant") {
      std::string input =  "/ab/aa/ab/cc/abd";
      REQUIRE(match(input, "/**/ab/**/abd"));
      REQUIRE(match(input, "/ab/**/cc/abd"));


      //      REQUIRE(match(input, "/**ab/**/abd**"));
      //      REQUIRE(match(input, "/**/ab/**/abd*"));
      //      REQUIRE(match(input, "/**/ab/**/abd**"));
      //      REQUIRE(match(input, "/**/ab/**/abd/**"));


      REQUIRE(match(input, "/**/ab/aa/**/cc/abd"));
      //
      REQUIRE(match(input, "/**/abc") == false);
      REQUIRE(match(input, "/**/ab/abd/**") == false);
      REQUIRE(match(input, "/**/ab/aa/**/cc/ab") == false);
      //
      REQUIRE(match(input, "/**/abd/**"));
      REQUIRE(match(input, "/**/abd/**/**/**"));
   }

   SECTION("Match all") {
      std::string pattern = "/**";
      //      REQUIRE(match("/", pattern));
      REQUIRE(match("/a", pattern));
      REQUIRE(match("/a/b", pattern));
      REQUIRE(match("/a/b/c", pattern));
   }

   SECTION("Input too short") {
      std::string pattern = "/**/abcd";
      REQUIRE(match("/abcd", pattern));
      REQUIRE(match("/abc", pattern) == false);
      REQUIRE(match("/foo/bar/baz/abc", pattern) == false);
      REQUIRE(match("/foo/bar/baz", pattern) == false);
   }

   SECTION("Input too long") {
      std::string pattern = "/**/abcd";
      REQUIRE(match("/abcd", pattern));
      REQUIRE(match("/abcde", pattern) == false);
      REQUIRE(match("/foo/bar/baz/abcde", pattern) == false);
      REQUIRE(match("/foo/bar/bcde", pattern) == false);
   }

   SECTION("Example") {
      std::string pattern = "/ab/*/ab/**/ac";
      REQUIRE(match("/ab/aa/ab/cc/ac", pattern));
      REQUIRE(match("/ab/aa/abc", pattern) == false);
      REQUIRE(match("/ab/aa/abd", pattern) == false);
      REQUIRE(match("/ab/aa/ab/cc/abd", pattern) == false);
      REQUIRE(match("/ab/abd", pattern) == false);
      REQUIRE(match("/ab/abd/cc/de/abd", pattern) == false);
      REQUIRE(match("/ab/abd/cc/de/bd", pattern) == false);
   }

   SECTION("Special Cases") {
      REQUIRE(match("/", "/*") == false);
      REQUIRE(match("/", "/**") == false);
   }

   SECTION("file test") {
      REQUIRE(match("/neutron/plugins/abc/dddddddddddd.py", "/neutron/plugins/*/d*dd*dd.py"));
      REQUIRE(match("/neutron/plugins/dabc/db.py", "/neutron/*/d*.py") == false);
      REQUIRE(match("/neutron/plugins/abcabcabc/db.py", "/neutron/plugins/abc*cb/db.py") == false);
   }

   SECTION("Reverse") {
      REQUIRE(match("db.py/aioöwerhioa/plugins/neutron/", "*.py/*/plugins/neutron/"));
   }

   SECTION("Additional Tests") {
      std::string pattern = "/**/f*oo*bar/baz";
      std::cout << "testing " << pattern << "\n";
      REQUIRE(match("/foobar/baz", pattern));
      REQUIRE(match("/foABoAAAoobar/baz", pattern));
      REQUIRE(match("/foABoAAAoobaAAAAbaAAAbar/baz", pattern));
      REQUIRE(match("/tmp/foABoAAAoobaAAAAbaAAAbar/baz", pattern));
      REQUIRE(match("/tmp/foobar/foABoAAAoobaAAAAbaAAAbar/baz", pattern));
      REQUIRE(match("/tmp/foobar/foABoAAAobaAAAAbaAAAbar/baz", pattern) == false);
      REQUIRE(match("/tmp/foobar/foABoAAAoobaAAAAbaAAAba/baz", pattern) == false);
   }

}
