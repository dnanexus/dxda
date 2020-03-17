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
