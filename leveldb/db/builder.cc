// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include <time.h>

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    // int tmp_counter=0;
    printf("----------Build New Table---------- \n");
    clock_t start_time, end_time;
    start_time = clock();
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      // if(tmp_counter == 0){
      //   printf("0th key in BuildTable: ");
      //   for(size_t index=0; index<key.size(); index++){
      //     printf(" %d ", int(key.data()[index]));
      //   }
      //   printf(" \n");
      // }
      builder->Add(key, iter->value());
      // tmp_counter ++;
    }
    end_time = clock();
    double total_time  =(double)(end_time - start_time)/CLOCKS_PER_SEC;
    fprintf(stderr, "Build New Table Time is: %.8f\n", total_time);
    // printf("After call builder->Add_Accelerate Func in builder.cc \n");
    
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
