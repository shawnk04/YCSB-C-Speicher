# YCSB-C

- Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)
- Based on basicthinker's YCSB-C (https://github.com/basicthinker/YCSB-C)
- Adds support for SplinterDB (https://github.com/vmware/splinterdb)
- Performance improvements
- New features (e.g. running Load and Workloads in separate executions)

## Quick Start for Debian-based systems

Install SplinterDB (https://github.com/vmware/splinterdb)

```
$ sudo apt-get install libtbb-dev librocksdb-dev libhiredis-dev
$ make
```

As the driver for Redis is linked by default, change the runtime library path
to include the hiredis library by:
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
```

Run Workload A with a [TBB](https://www.threadingbuildingblocks.org)-based
implementation of the database, for example:
```
./ycsbc -db splinterdb -threads 4 -L workloads/load.spec -W workloads/workloada.spec
```

Note that we do not have load and run commands as the original YCSB. Specify
how many records to load by the recordcount property. Reference properties
files in the workloads dir.

