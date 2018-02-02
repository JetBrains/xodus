# <img src="https://raw.githubusercontent.com/wiki/jetbrains/xodus/xodus.png" width=160>

[JMH Gradle Plugin](https://github.com/melix/jmh-gradle-plugin) is used to build and run benchmarks. Benchmark results are obtained on a PC running under Windows 7 with Intel(R) Core(TM) i7-3770 3.4 GHz CPU and 64-bit OpenJDK build 1.8.0_131-1-ojdkbuild-b11 with the following parameters: `-Xms1g -Xmx1g`. To get results in your environment, run in the project root directory:

    ./gradlew clean jar jmh

#### Tokyo Cabinet Benchmark

[Tokyo Cabinet Benchmark](http://fallabs.com/tokyocabinet/benchmark.pdf) is useful for comparing the performance of key/value storages. For one million 8-char string keys ('00000000', '00000001', etc.) and values equal to the keys, four operations are measured:

1. writing all key/value pairs in ascending order;
2. writing all key/value pairs in random order;
3. reading all key/value pairs in ascending order;
4. reading all key/value pairs in random order.

Currently, benchmark results are available for [Xodus stores](https://github.com/JetBrains/xodus/wiki/Environments#stores) with key prefixing (Patricia tree) and without (BTree), [MapDb](https://github.com/jankotek/MapDB) tree map, [Chronicle Map](https://github.com/OpenHFT/Chronicle-Map), [H2 MVStore Map](http://www.h2database.com/html/mvstore.html), [LMDB JNI](https://github.com/deephacks/lmdbjni) and [Akiban PersistIt](https://github.com/pbeaman/persistit). All the scores are in seconds per single benchmark run for one million keys. Excerpt of the output of the build running benchmarks is as follows:

```
Benchmark                                                             Mode  Cnt    Score   Error   Units
chronicle.JMHChronicleMapTokyoCabinetReadBenchmark.randomRead           ss   10    0.553 ± 0.020    s/op
chronicle.JMHChronicleMapTokyoCabinetReadBenchmark.successiveRead       ss   10    0.180 ± 0.028    s/op
chronicle.JMHChronicleMapTokyoCabinetWriteBenchmark.randomWrite         ss   10    0.726 ± 0.037    s/op
chronicle.JMHChronicleMapTokyoCabinetWriteBenchmark.successiveWrite     ss   10    0.675 ± 0.008    s/op
env.JMHEnvTokyoCabinetReadBenchmark.randomRead                          ss   10    1.880 ± 0.016    s/op
env.JMHEnvTokyoCabinetReadBenchmark.successiveRead                      ss   10    0.156 ± 0.013    s/op
env.JMHEnvTokyoCabinetWriteBenchmark.randomWrite                        ss   10    1.888 ± 0.059    s/op
env.JMHEnvTokyoCabinetWriteBenchmark.successiveWrite                    ss   10    0.462 ± 0.022    s/op
env.JMHEnvWithPrefixingTokyoCabinetReadBenchmark.randomRead             ss   10    0.994 ± 0.004    s/op
env.JMHEnvWithPrefixingTokyoCabinetReadBenchmark.successiveRead         ss   10    0.240 ± 0.008    s/op
env.JMHEnvWithPrefixingTokyoCabinetWriteBenchmark.randomWrite           ss   10    1.486 ± 0.366    s/op
env.JMHEnvWithPrefixingTokyoCabinetWriteBenchmark.successiveWrite       ss   10    0.461 ± 0.050    s/op
h2.JMH_MVStoreTokyoCabinetReadBenchmark.randomRead                      ss   10   13.092 ± 0.029    s/op
h2.JMH_MVStoreTokyoCabinetReadBenchmark.successiveRead                  ss   10    0.109 ± 0.003    s/op
h2.JMH_MVStoreTokyoCabinetWriteBenchmark.randomWrite                    ss   10    1.860 ± 0.006    s/op
h2.JMH_MVStoreTokyoCabinetWriteBenchmark.successiveWrite                ss   10    0.517 ± 0.097    s/op
lmdb.JMH_LMDBTokyoCabinetReadBenchmark.randomRead                       ss   10    0.825 ± 0.004    s/op
lmdb.JMH_LMDBTokyoCabinetReadBenchmark.successiveRead                   ss   10    0.098 ± 0.001    s/op
lmdb.JMH_LMDBTokyoCabinetWriteBenchmark.randomWrite                     ss   10    0.831 ± 0.003    s/op
lmdb.JMH_LMDBTokyoCabinetWriteBenchmark.successiveWrite                 ss   10    0.178 ± 0.001    s/op
mapdb.JMHMapDbTokyoCabinetReadBenchmark.randomRead                      ss   10    7.154 ± 0.036    s/op
mapdb.JMHMapDbTokyoCabinetReadBenchmark.successiveRead                  ss   10    0.126 ± 0.004    s/op
mapdb.JMHMapDbTokyoCabinetWriteBenchmark.randomWrite                    ss   10    9.075 ± 0.030    s/op
mapdb.JMHMapDbTokyoCabinetWriteBenchmark.successiveWrite                ss   10   11.982 ± 0.132    s/op
persistit.JMHPersistItTokyoCabinetReadBenchmark.randomRead              ss   10    1.316 ± 0.020    s/op
persistit.JMHPersistItTokyoCabinetReadBenchmark.successiveRead          ss   10    0.857 ± 0.301    s/op
persistit.JMHPersistItTokyoCabinetWriteBenchmark.randomWrite            ss   10    1.763 ± 0.019    s/op
persistit.JMHPersistItTokyoCabinetWriteBenchmark.successiveWrite        ss   10    0.632 ± 0.059    s/op                                                                         
```

The same results in table form:
<table>
<tr><td></td><th>Random Read</th><th>Successive Read</th><th>Random Write</th><th>Successive Write</th></tr>
<tr><th><a href="https://github.com/JetBrains/xodus/wiki/Environments#stores">Xodus store</a> with key prefixing (Patricia)</th><td>0.994</td><td>0.240</td><td>1.486</td><td>0.461</td></tr>
<tr><th><a href="https://github.com/JetBrains/xodus/wiki/Environments#stores">Xodus store</a> without key prefixing (BTree)</th><td>1.880</td><td>0.156</td><td>1.888</td><td>0.462</td></tr>
<tr><th><a href="http://www.oracle.com/us/products/database/berkeley-db/je/overview/index.html">BDB JE</a> database with key prefixing</th><td>3.493</td><td>1.307</td><td>5.937</td><td>3.416</td></tr>
<tr><th><a href="http://www.oracle.com/us/products/database/berkeley-db/je/overview/index.html">BDB JE</a> database  without key prefixing</th><td>3.288</td><td>1.251</td><td>5.831</td><td>3.458</td></tr>
<tr><th><a href="https://github.com/jankotek/MapDB">MapDB</a> tree map</th><td>7.154</td><td>0.126</td><td>9.075</td><td>11.982</td></tr>
<tr><th><a href="https://github.com/OpenHFT/Chronicle-Map">Chronicle Map</a></th><td>0.553</td><td>0.180</td><td>0.726</td><td>0.675</td></tr>
<tr><th><a href="http://www.h2database.com/html/mvstore.html">H2 MVStore Map</a></th><td>13.092</td><td>0.109</td><td>1.860</td><td>0.517</td></tr>
<tr><th><a href="https://github.com/deephacks/lmdbjni">LMDB JNI</a></th><td>0.825</td><td>0.098</td><td>0.831</td><td>0.178</td></tr>
<tr><th><a href="https://github.com/pbeaman/persistit">Akiban PersistIt</a></th><td>1.316</td><td>0.857</td><td>1.763</td><td>0.632</td></tr>
</table>

Results for [BerkeleyDb JE](http://www.oracle.com/us/products/database/berkeley-db/je/overview/index.html) are obtained in a similar environment as above (another JRE used), but the code of benchmark cannot be distributed under [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).