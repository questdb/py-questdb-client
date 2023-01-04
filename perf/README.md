# Profiling with Linux Perf

https://juanjose.garciaripoll.com/blog/profiling-code-with-linux-perf/index.html

```bash
$ TEST_QUESTDB_PATCH_PATH=1 perf record -g --call-graph dwarf python3 test/benchmark.py -v TestBencharkPandas.test_string_encoding_1m
test_string_encoding_1m (__main__.TestBencharkPandas.test_string_encoding_1m) ... Time: 4.682273147998785, size: 4593750000
ok

----------------------------------------------------------------------
Ran 1 test in 10.166s

OK
[ perf record: Woken up 1341 times to write data ]
Warning:
Processed 54445 events and lost 91 chunks!

Check IO/CPU overload!

[ perf record: Captured and wrote 405.575 MB perf.data (50622 samples) ]
```

# Rendering results

```bash
$ perf script | python3 perf/gprof2dot.py --format=perf | dot -Tsvg > perf/profile_graph.svg
$ (cd perf && python3 -m http.server)
```