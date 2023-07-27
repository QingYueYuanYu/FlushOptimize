// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  size_t tmp_buffer_length;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    // printf("Current min_length: %d \n", min_length);
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
    // printf("Current shared: %d \n", shared);
  } else {
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;
  // printf("Current non_shared: %d \n", non_shared);

  // Add "<shared><non_shared><value_size>" to buffer_
  tmp_buffer_length = buffer_.size();
  PutVarint32(&buffer_, shared);
  // printf("PutVarint32OnDevice about shared result buffer length varying from %d to %d \n", tmp_buffer_length, buffer_.size());
  tmp_buffer_length = buffer_.size();
  PutVarint32(&buffer_, non_shared);
  // printf("PutVarint32OnDevice about non_shared result buffer length varying from %d to %d with non_shared: %d \n", tmp_buffer_length, buffer_.size(), non_shared);
  tmp_buffer_length = buffer_.size();
  PutVarint32(&buffer_, value.size());
  // printf("PutVarint32OnDevice about value result buffer length varying from %d to %d \n", tmp_buffer_length, buffer_.size());

  tmp_buffer_length = buffer_.size();
  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  // printf("Memcpy about non-shared key result buffer length varying from %d to %d \n", tmp_buffer_length, buffer_.size());
  tmp_buffer_length = buffer_.size();
  buffer_.append(value.data(), value.size());
  // printf("PutVarint32OnDevice about value result buffer length varying from %d to %d \n", tmp_buffer_length, buffer_.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
  
  // printf("Current counter: %d  Current buffer: %s  buffer[0]: %c  buffer[10] ascii: %d  Current buffer size: %zu \n", counter_, buffer_.c_str(), buffer_.c_str()[10], int(buffer_.c_str()[0]), buffer_.size());
  // printf("Current restarts_:  ");
  for(size_t index=0; index<restarts_.size(); index++){
    // printf("restarts_[%zu]: %zu  ", index, restarts_[index]);
  }
  // printf("\n");
  size_t test_begin=204800;
  size_t test_end=204850;
  // printf("Buffer value ascii from %zu to %zu: ", test_begin, test_end);
  for(size_t test_index=test_begin; test_index<test_end && test_index<buffer_.size(); test_index++){
    if(test_index%10==0){
      // printf("     ");
    }
    // printf(" %d ", int(buffer_.c_str()[test_index]));
  }
  // printf(" \n");

  // printf("Current key: ");
  for(size_t index=0; index<key.size(); index++){
    // printf(" %d ", int(key.data()[index]));
  }
  // printf(" \n");
  
  // printf("Current last_key(after add): ");
  for(size_t index=0; index<last_key_.size(); index++){
    // printf(" %d ", int(last_key_.c_str()[index]));
  }
  // printf(" \n");
}

}  // namespace leveldb
