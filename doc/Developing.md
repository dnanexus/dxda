# Running tests

Assuming we are at the git root directory, then here is how to run tests:
```
# log into staging
$ dx login --staging

# go into the dxfuse_test_data project
$ dx select dxfuse_test_data

# go to the tests directory
$ cd test

# build the test applets and dx-downoad-agent executable
$ make

# run a small test
$ ../scripts/run_tests.py --test correctness

# run a 10 minute benchmark
$ ../scripts/run_tests.py --test bench

# run a large test with 2 TiB of storage
$ ../scripts/run_tests.py --test large_data

```

In order to test on the local machine, not on the cloud, go to directory `test/local`. It has two tests:


| file name              |  what it does                          |  disk size | number of parts |
| ----                   |  ---                                   | ---        | ---             |
| `regular_file_test.sh` | small scale test for regular files     | 138 MB | 559 |
| `symlink_test.sh`      | test with five moderate sized symlinks | 98 MB | 10 |