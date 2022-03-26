//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"

using namespace std;

typedef struct WorkloadProperties {
  string filename;
  bool preloaded;
  utils::Properties props;
} WorkloadProperties;


void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], utils::Properties &props, WorkloadProperties &load_workload, vector<WorkloadProperties> &run_workloads);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  db->Init();
  ycsbc::Client client(*db, *wl);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
  }
  db->Close();
  return oks;
}

std::map<string, string> default_props = {
  {"threadcount", "1"},
  {"dbname", "basic"},

  //
  // splinterdb config defaults
  //
  {"splinterdb.filename", "splinterdb.db"},
  {"splinterdb.cache_size_mb", "4096"},
  {"splinterdb.disk_size_gb", "128"},

  {"splinterdb.max_key_size", "24"},
  {"splinterdb.use_log", "1"},

  // All these options use splinterdb's internal defaults
  {"splinterdb.page_size", "0"},
  {"splinterdb.extent_size", "0"},
  {"splinterdb.io_flags", "0"},
  {"splinterdb.io_perms", "0"},
  {"splinterdb.io_async_queue_depth", "0"},
  {"splinterdb.cache_use_stats", "0"},
  {"splinterdb.cache_logfile", "0"},
  {"splinterdb.btree_rough_count_height", "0"},
  {"splinterdb.filter_remainder_size", "0"},
  {"splinterdb.filter_index_size", "0"},
  {"splinterdb.memtable_capacity", "0"},
  {"splinterdb.fanout", "0"},
  {"splinterdb.max_branches_per_node", "0"},
  {"splinterdb.use_stats", "0"},
  {"splinterdb.reclaim_threshold", "0"},

  {"rocksdb.database_filename", "rocksdb.db"},
};

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  WorkloadProperties load_workload;
  vector<WorkloadProperties> run_workloads;
  ParseCommandLine(argc, argv, props, load_workload, run_workloads);

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  vector<future<int>> actual_ops;
  int total_ops;
  int sum;
  utils::Timer<double> timer;

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props, load_workload.preloaded);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  ycsbc::CoreWorkload wl;

  // Perform the Load phase
  wl.InitLoadWorkload(load_workload.props, load_workload.preloaded);
  if (!load_workload.preloaded) {
    total_ops = stoi(load_workload.props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    if (total_ops) {
      timer.Start();
      for (int i = 0; i < num_threads; ++i) {
        actual_ops.emplace_back(async(launch::async,
                                      DelegateClient, db, &wl, total_ops / num_threads, true));
      }
      assert((int)actual_ops.size() == num_threads);

      sum = 0;
      for (auto &n : actual_ops) {
        assert(n.valid());
        sum += n.get();
      }
      double load_duration = timer.End();
      cerr << "# Loading records:\t" << sum << endl;
      cerr << "# Load throughput (KTPS)" << endl;
      cerr << props["dbname"] << '\t' << load_workload.filename << '\t' << num_threads << '\t';
      cerr << total_ops / load_duration / 1000 << endl;
    }
  }

  // Perform any Run phases
  for (unsigned int i = 0; i < run_workloads.size(); i++) {
    auto workload = run_workloads[i];
    wl.InitRunWorkload(workload.props);
    actual_ops.clear();
    total_ops = stoi(workload.props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    if (total_ops) {
      timer.Start();
      for (int i = 0; i < num_threads; ++i) {
        actual_ops.emplace_back(async(launch::async,
                                      DelegateClient, db, &wl, total_ops / num_threads, false));
      }
      assert((int)actual_ops.size() == num_threads);

      sum = 0;
      for (auto &n : actual_ops) {
        assert(n.valid());
        sum += n.get();
      }
      double run_duration = timer.End();
      cerr << "# Transaction count:\t" << sum << endl;
      cerr << "# Transaction throughput (KTPS)" << endl;
      cerr << props["dbname"] << '\t' << workload.filename << '\t' << num_threads << '\t';
      cerr << total_ops / run_duration / 1000 << endl;
    }
  }

  delete db;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props, WorkloadProperties &load_workload, vector<WorkloadProperties> &run_workloads) {
  bool saw_load_workload = false;
  WorkloadProperties *last_workload = NULL;
  int argindex = 1;

  for (auto const & [key, val] : default_props) {
    props.SetProperty(key, val);
  }

  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-W") == 0
               || strcmp(argv[argindex], "-P") == 0
               || strcmp(argv[argindex], "-L") == 0) {
      WorkloadProperties workload;
      workload.preloaded = strcmp(argv[argindex], "-P") == 0;
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      workload.filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        workload.props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
      if (strcmp(argv[argindex-2], "-W") == 0) {
        run_workloads.push_back(workload);
        last_workload = &run_workloads[run_workloads.size()-1];
      } else if (saw_load_workload) {
        UsageMessage(argv[0]);
        exit(0);
      } else {
        saw_load_workload = true;
        load_workload = workload;
        last_workload = &load_workload;
      }
    } else if (strcmp(argv[argindex], "-p") == 0
               || strcmp(argv[argindex], "-w") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      std::string propkey = argv[argindex];
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      std::string propval = argv[argindex];
      if (strcmp(argv[argindex-2], "-w") == 0) {
        if (last_workload) {
          last_workload->props.SetProperty(propkey, propval);
        } else {
          UsageMessage(argv[0]);
          exit(0);
        }
      } else {
        props.SetProperty(propkey, propval);
      }
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc || !saw_load_workload) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << "-L <load-workload.spec> [-W run-workload.spec] ..." << endl;
  cout << "       Perform the given Load workload, then each Run workload" << endl;
  cout << "Usage: " << command << " [options]" << "-P <load-workload.spec> [-W run-workload.spec] ... " << endl;
  cout << "       Perform each given Run workload on a database that has been preloaded with the given Load workload" << endl;
  cout << "Options:" << endl;
  cout << "  -threads <n>: execute using <n> threads (default: " << default_props["threadcount"] << ")" << endl;
  cout << "  -db <dbname>: specify the name of the DB to use (default: " << default_props["dbname"] << ")" << endl;
  cout << "  -L <file>: Initialize the database with the specified Load workload" << endl;
  cout << "  -P <file>: Indicates that the database has been preloaded with the specified Load workload" << endl;
  cout << "  -W <file>: Perform the Run workload specified in <file>" << endl;
  cout << "  -p <prop> <val>: set property <prop> to value <val>" << endl;
  cout << "  -w <prop> <val>: set a property in the previously specified workload" << endl;
  cout << "Exactly one Load workload is allowed, but multiple Run workloads may be given.." << endl;
  cout << "Run workloads will be executed in the order given on the command line." << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

