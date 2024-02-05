/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

// 2024-01-24
// Ubuntu 22.04.3 LTS, AMD Ryzen 9 7950X3D 16-Core Processor
// JMH version: 1.36
// VM version: JDK 21.0.2, Java HotSpot(TM) 64-Bit Server VM, 21.0.2+13-LTS-58
// VM invoker: /home/kirill/.sdkman/candidates/java/21.0.2-oracle/bin/java
// VM options: -Djava.awt.headless=true -XX:MaxDirectMemorySize=82g -XX:+HeapDumpOnOutOfMemoryError --add-modules=jdk.incubator.vector --enable-preview -Xmx16g -Dfile.encoding=UTF-8 -Duser.country -Duser.language=en -Duser.variant
// Warmup: 5 iterations, 10 s each
// Measurement: 5 iterations, 10 s each

// Benchmark                               (distanceFunction)  Mode  Cnt     Score    Error  Units
// computeDistance_heap_array_1                      l2-cur-j  avgt   25    13.656 ±  0.315  ns/op
// computeDistance_heap_array_1                      l2-new-k  avgt   25    44.482 ±  4.681  ns/op
// computeDistance_heap_array_1                      l2-new-j  avgt   25    20.147 ±  8.867  ns/op
// computeDistance_heap_array_1                      ip-cur-j  avgt   25    18.969 ±  6.871  ns/op
// computeDistance_heap_array_1                      ip-new-k  avgt   25    26.011 ±  9.648  ns/op
// computeDistance_heap_array_1                      ip-new-j  avgt   25    21.429 ±  9.173  ns/op

// computeDistance_heap_array_4                      l2-cur-j  avgt   25   136.489 ± 40.330  ns/op
// computeDistance_heap_array_4                      l2-new-k  avgt   25   116.688 ± 50.086  ns/op
// computeDistance_heap_array_4                      l2-new-j  avgt   25   137.288 ± 61.257  ns/op
// computeDistance_heap_array_4                      ip-cur-j  avgt   25   109.668 ± 33.747  ns/op
// computeDistance_heap_array_4                      ip-new-k  avgt   25   191.941 ± 29.656  ns/op
// computeDistance_heap_array_4                      ip-new-j  avgt   25   145.178 ± 54.418  ns/op

// computeDistance_heap_array_4_batch                l2-cur-j  avgt   25  2725.085 ± 26.417  ns/op
// computeDistance_heap_array_4_batch                l2-new-k  avgt   25   113.120 ± 52.378  ns/op
// computeDistance_heap_array_4_batch                l2-new-j  avgt   25   126.899 ± 52.204  ns/op
// computeDistance_heap_array_4_batch                ip-cur-j  avgt   25  2707.223 ± 32.452  ns/op
// computeDistance_heap_array_4_batch                ip-new-k  avgt   25   111.451 ± 51.754  ns/op
// computeDistance_heap_array_4_batch                ip-new-j  avgt   25   136.713 ± 51.090  ns/op

// computeDistance_native_segment_1                  l2-cur-j  avgt   25    32.674 ± 14.303  ns/op
// computeDistance_native_segment_1                  l2-new-k  avgt   25    29.257 ± 10.033  ns/op
// computeDistance_native_segment_1                  l2-new-j  avgt   25    45.969 ± 10.563  ns/op
// computeDistance_native_segment_1                  ip-cur-j  avgt   25    33.057 ±  9.898  ns/op
// computeDistance_native_segment_1                  ip-new-k  avgt   25    26.914 ± 10.964  ns/op
// computeDistance_native_segment_1                  ip-new-j  avgt   25    30.235 ± 11.799  ns/op

// computeDistance_native_segment_4                  l2-cur-j  avgt   25   140.775 ± 46.405  ns/op
// computeDistance_native_segment_4                  l2-new-k  avgt   25   197.088 ± 45.773  ns/op
// computeDistance_native_segment_4                  l2-new-j  avgt   25   249.489 ± 60.843  ns/op
// computeDistance_native_segment_4                  ip-cur-j  avgt   25   151.710 ± 40.663  ns/op
// computeDistance_native_segment_4                  ip-new-k  avgt   25   285.272 ± 54.375  ns/op
// computeDistance_native_segment_4                  ip-new-j  avgt   25   140.465 ± 56.175  ns/op

// computeDistance_native_segment_4_batch            l2-cur-j  avgt   25  2712.345 ± 28.513  ns/op
// computeDistance_native_segment_4_batch            l2-new-k  avgt   25   156.565 ± 54.092  ns/op
// computeDistance_native_segment_4_batch            l2-new-j  avgt   25    96 .043 ± 43.784  ns/op
// computeDistance_native_segment_4_batch            ip-cur-j  avgt   25  2707.009 ± 12.506  ns/op
// computeDistance_native_segment_4_batch            ip-new-k  avgt   25   118.555 ± 50.070  ns/op
// computeDistance_native_segment_4_batch            ip-new-j  avgt   25   180.573 ± 32.091  ns/op

@SuppressWarnings("unused")
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, warmups = 1) // uncomment it to make things faster
public class DistanceComputationBenchmark {
    private static final int vectorSize = 128;

    @SuppressWarnings("FieldCanBeLocal")
    private final int numVectors = 100_000;
    private int currentVector = 0;

    private float[][] vectorsArr;
    private float[] resultArr;

    private Arena arena;

    private MemorySegment[] vectorsSeg;

    @Param({
            "l2-cur-j",
            "l2-new-k",

            "ip-cur-j",
            "ip-new-k",
    })
    public String distanceFunction;

    private final Map<String, DistanceFunction> distanceFunctions = Map.of(
            "l2-cur-j", new L2DistanceFunction(),
            "l2-new-k", new L2DistanceFunctionNew(),

            "ip-cur-j", new DotDistanceFunction(),
            "ip-new-k", new DotDistanceFunctionNew()
    );

    private DistanceFunction distanceFun;

    @Setup(Level.Iteration)
    public void init() {
        var rnd = ThreadLocalRandom.current();
        arena = Arena.ofShared();

        vectorsArr = new float[numVectors][];
        resultArr = new float[numVectors];

        vectorsSeg = new MemorySegment[numVectors];

        for (int i = 0; i < numVectors; i++) {
            vectorsArr[i] = new float[vectorSize];
            vectorsSeg[i] = arena.allocate(vectorSize * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
        }

        for (int dimensionIdx = 0; dimensionIdx < vectorSize; dimensionIdx++) {
            for (int vectorIdx = 0; vectorIdx < numVectors; vectorIdx++) {
                vectorsArr[vectorIdx][dimensionIdx] = rnd.nextFloat();
                vectorsSeg[vectorIdx].setAtIndex(ValueLayout.JAVA_FLOAT, dimensionIdx, rnd.nextFloat());
            }
        }

        distanceFun = distanceFunctions.get(distanceFunction);
        currentVector = 0;
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        arena.close();
        arena = null;
    }

    @Benchmark
    public float computeDistance_heap_array_1() {
        return distanceFun.computeDistance(vectorsArr[currentVector++ % numVectors], 0, vectorsArr[currentVector++ % numVectors], 0, vectorSize);
    }

    @Benchmark
    public float computeDistance_heap_array_4() {
        var res = distanceFun.computeDistance(vectorsArr[currentVector++ % numVectors], 0, vectorsArr[currentVector++ % numVectors], 0, vectorSize);
        res += distanceFun.computeDistance(vectorsArr[currentVector++ % numVectors], 0, vectorsArr[currentVector++ % numVectors], 0, vectorSize);
        res += distanceFun.computeDistance(vectorsArr[currentVector++ % numVectors], 0, vectorsArr[currentVector++ % numVectors], 0, vectorSize);
        res += distanceFun.computeDistance(vectorsArr[currentVector++ % numVectors], 0, vectorsArr[currentVector++ % numVectors], 0, vectorSize);
        return res;
    }

    @Benchmark
    public void computeDistance_heap_array_4_batch() {
        distanceFun.computeDistance(
                vectorsArr[currentVector++ % numVectors], 0,
                vectorsArr[currentVector++ % numVectors], 0,
                vectorsArr[currentVector++ % numVectors], 0,
                vectorsArr[currentVector++ % numVectors], 0,
                vectorsArr[currentVector++ % numVectors], 0,
                resultArr, vectorSize
        );
    }

    @Benchmark
    public float computeDistance_native_segment_1() {
        return distanceFun.computeDistance(vectorsSeg[currentVector++ % numVectors], 0, vectorsSeg[currentVector++ % numVectors], 0, vectorSize);
    }
    
    @Benchmark
    public float computeDistance_native_segment_4() {
        var res = distanceFun.computeDistance(vectorsSeg[currentVector++ % numVectors], 0, vectorsSeg[currentVector++ % numVectors], 0, vectorSize);
        res += distanceFun.computeDistance(vectorsSeg[currentVector++ % numVectors], 0, vectorsSeg[currentVector++ % numVectors], 0, vectorSize);
        res += distanceFun.computeDistance(vectorsSeg[currentVector++ % numVectors], 0, vectorsSeg[currentVector++ % numVectors], 0, vectorSize);
        res += distanceFun.computeDistance(vectorsSeg[currentVector++ % numVectors], 0, vectorsSeg[currentVector++ % numVectors], 0, vectorSize);
        return res;
    }

    @Benchmark
    public void computeDistance_native_segment_4_batch() {
        distanceFun.computeDistance(
                vectorsSeg[currentVector++ % numVectors], 0,
                vectorsSeg[currentVector++ % numVectors], 0,
                vectorsSeg[currentVector++ % numVectors], 0,
                vectorsSeg[currentVector++ % numVectors], 0,
                vectorsSeg[currentVector++ % numVectors], 0,
                vectorSize, resultArr
        );
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(DistanceComputationBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}