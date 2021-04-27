#include "cas/path_matcher.hpp"
#include "cas/key_encoding.hpp"
#include "cas/search_key.hpp"
#include <cassert>
#include <iostream>
#include <stdexcept>


cas::path_matcher::PrefixMatch cas::path_matcher::MatchPathIncremental(
      const cas::QueryBuffer& path,
      const std::vector<std::byte>& query_path,
      size_t len_path,
      State& s) {

   uint16_t len_qpath = static_cast<uint16_t>(query_path.size());
   while (s.ppos_ < len_path &&
          s.desc_ppos_ < len_path &&
          path[s.ppos_] != cas::kNullByte) {
      if (s.qpos_ < query_path.size() && path[s.ppos_] == query_path[s.qpos_]) {
         // simple pattern symbol matches input symbol
         ++s.ppos_;
         ++s.qpos_;
         if (query_path[s.qpos_-1] == kPathSep){
           // we matchted the path separator, forget about the last * wildcard
           s.star_qpos_ = -1;
         }
      } else if ((query_path[s.qpos_] == cas::kByteChildAxis && s.qpos_+1 >= len_qpath)
                 || (s.qpos_+1 < len_qpath &&
                    query_path[s.qpos_] == cas::kByteChildAxis &&
                    query_path[s.qpos_+1] != cas::kByteChildAxis)) {
         // we found only one star (* wildcard)
         // remember its position and skip (accept) it for the moment
         s.star_ppos_ = s.ppos_;
         s.star_qpos_ = s.qpos_+1;
         s.qpos_ = s.qpos_+1;
      } else if (s.star_qpos_ != -1) {
         // the * wildcard must match a longer substring
         if (path[s.star_ppos_] == cas::kPathSep) {
            // do not allow to take a kPathSep into the one star wildcard
            s.star_qpos_ = -1;
         } else {
            s.star_ppos_ += 1;
            s.ppos_ = s.star_ppos_;
            s.qpos_ = s.star_qpos_;
         }
      } else if (s.qpos_+1 < len_qpath &&
                query_path[s.qpos_] == cas::kByteChildAxis &&
                query_path[s.qpos_+1] == cas::kByteChildAxis) {
         // we found /**/ (i.e., descendant-or-self axis)
         // remember where we found the last descendant-or-self axis
         s.desc_ppos_ = s.ppos_;
         s.desc_qpos_ = s.qpos_+3;
         s.qpos_ = s.qpos_+3;
      } else if (s.desc_qpos_ != -1) {
         // the descendant-or-self axis must match a larger substring
         if (path[s.desc_ppos_] == cas::kPathSep) {
            s.ppos_ = s.desc_ppos_ + 1;
            s.qpos_ = s.desc_qpos_;
            ++s.desc_ppos_;
         } else if (path[s.desc_ppos_] == cas::kNullByte) {
            // matched the descendant-or-self axis until the end
            s.ppos_ = s.desc_ppos_;
            s.qpos_ = s.desc_qpos_-1;
         } else {
            ++s.desc_ppos_;
         }
      } else {
         return PrefixMatch::MISMATCH;
      }
   }

   if (s.ppos_ >= len_path || s.desc_ppos_ >= len_path) {
      return PrefixMatch::INCOMPLETE;
   }

   // we reached the end of a full path. now we can determine
   // if we fully matched it or not
   assert(path[s.ppos_] == cas::kNullByte); // NOLINT

   // this is needed if /a/<star><star> should match /a
   while(s.qpos_+2 < len_qpath
         && query_path[s.qpos_] == cas::kPathSep
         && query_path[s.qpos_+1] == cas::kByteChildAxis
         && query_path[s.qpos_+2] == cas::kByteChildAxis){
      s.qpos_ += 3;
   }

   return (s.qpos_ == query_path.size()) ? PrefixMatch::MATCH : PrefixMatch::MISMATCH;
}


bool cas::path_matcher::MatchPath(
      const cas::QueryBuffer& path,
      const std::vector<std::byte>& query_path,
      size_t len_path) {
   State s;
   return MatchPathIncremental(path, query_path, len_path, s) == PrefixMatch::MATCH;
}


void cas::path_matcher::State::Dump() const {
   std::cout << "ppos_: " << ppos_ << std::endl;
   std::cout << "qpos_: " << qpos_ << std::endl;
   std::cout << "desc_ppos_: " << desc_ppos_ << std::endl;
   std::cout << "desc_qpos_: " << desc_qpos_ << std::endl;
}


std::string cas::path_matcher::ToString(PrefixMatch m) {
   switch (m) {
   case PrefixMatch::MATCH:      return "MATCH";
   case PrefixMatch::MISMATCH:   return "MISMATCH";
   case PrefixMatch::INCOMPLETE: return "INCOMPLETE";
   }
   throw std::runtime_error{"impossible"};
}
