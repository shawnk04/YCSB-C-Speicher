//
//  redis_db.h
//  YCSB-C
//

#ifndef YCSB_C_SPLINTER_DB_H_
#define YCSB_C_SPLINTER_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "core/properties.h"

extern "C" {

#include "platform.h"
#include "splinter.h"
#include "task.h"
#include "rc_allocator.h"
#include "clockcache.h"
//#include "test.h"
}

using std::cout;
using std::endl;

namespace ycsbc {

class SplinterDB : public DB {
 public:
  SplinterDB();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);
  
 private:
  io_config             io_cfg;
  rc_allocator_config   allocator_cfg;
  clockcache_config     cache_cfg;
  shard_log_config      log_cfg;
  platform_heap_handle  hh;
  platform_heap_id      hid;
  data_config          *data_cfg;
  splinter_config      *splinter_cfg;
  platform_io_handle   *io;
  rc_allocator          al;
  clockcache           *cc;
  splinter_handle      *spl;
  
};

} // ycsbc

#endif // YCSB_C_SPLINTER_DB_H_

