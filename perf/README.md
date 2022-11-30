# Profiling with Linux Perf

https://juanjose.garciaripoll.com/blog/profiling-code-with-linux-perf/index.html

```bash
$ TEST_QUESTDB_PATCH_PATH=1 TEST_QUESTDB_PROFILE=0 perf record -g --call-graph dwarf python3 test/test.py -v TestBencharkPandas.test_mixed_10m
test_mixed_10m (__main__.TestBencharkPandas.test_mixed_10m) ... Time: 2.128126113999315, size: 558055572
ok

----------------------------------------------------------------------
Ran 1 test in 10.188s

OK
[ perf record: Woken up 1337 times to write data ]
Warning:
Processed 55721 events and lost 107 chunks!

Check IO/CPU overload!

[ perf record: Captured and wrote 402.922 MB perf.data (50252 samples) ]
```

# Rendering results

```bash
$ perf script | python3 perf/gprof2dot.py --format=perf | dot -Tsvg > perf/profile_graph.svg
$ (cd perf && python3 -m http.server)
```