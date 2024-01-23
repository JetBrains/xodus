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

// Results measured:

// 2024-01-12
// Apple M1 Max, MaсOs 14.2.1 (23C71)
// JMH version: 1.36
// VM version: JDK 21.0.1, OpenJDK 64-Bit Server VM, 21.0.1+12-LTS
// VM options: -Djava.awt.headless=true --enable-preview --add-modules=jdk.incubator.vector --enable-preview --add-modules=jdk.incubator.vector -Dfile.encoding=UTF-8 -Duser.country=NL -Duser.language=en -Duser.variant

// Warmup: 5 iterations, 10 s each
// Measurement: 5 iterations, 10 s each
// Threads: 1 thread, will synchronize iterations

//                                         (distanceFunction)  Mode  Cnt    Score    Error  Units
// computeDistance_heap_array_1                            l2  avgt    5   14,098  ± 0,814  ns/op
// computeDistance_heap_array_1                        l2-new  avgt    5   13,716  ± 0,104  ns/op
// computeDistance_heap_array_1                            ip  avgt    5   11,549  ± 0,594  ns/op
// computeDistance_heap_array_1                        ip-new  avgt    5   11,632  ± 0,543  ns/op

// computeDistance_heap_array_4                            l2  avgt    5   64,918 ±  0,165  ns/op
// computeDistance_heap_array_4                        l2-new  avgt    5   56,137 ±  0,408  ns/op
// computeDistance_heap_array_4                            ip  avgt    5   56,997 ±  0,206  ns/op
// computeDistance_heap_array_4                        ip-new  avgt    5   44,678 ±  0,038  ns/op

// computeDistance_heap_array_4_batch                      l2  avgt    5   36,331 ±  0,924  ns/op
// computeDistance_heap_array_4_batch                  l2-new  avgt    5   35,607 ±  0,581  ns/op
// computeDistance_heap_array_4_batch                      ip  avgt    5   36,085 ±  0,314  ns/op
// computeDistance_heap_array_4_batch                  ip-new  avgt    5   37,037 ±  1,383  ns/op

// computeDistance_native_segment_1                        l2  avgt    5   23,344 ±  0,688  ns/op
// computeDistance_native_segment_1                    l2-new  avgt    5   16,823 ±  0,352  ns/op
// computeDistance_native_segment_1                        ip  avgt    5   20,724 ±  0,333  ns/op
// computeDistance_native_segment_1                    ip-new  avgt    5   16,642 ±  0,970  ns/op

// computeDistance_native_segment_4                        l2  avgt    5   97,697 ±  4,863  ns/op
// computeDistance_native_segment_4                    l2-new  avgt    5   80,522 ±  1,557  ns/op
// computeDistance_native_segment_4                        ip  avgt    5   96,511 ±  0,914  ns/op
// computeDistance_native_segment_4                    ip-new  avgt    5  268,011 ±  7,819  ns/op

// computeDistance_native_segment_4_batch                  l2  avgt    5   63,883 ±  1,094  ns/op
// computeDistance_native_segment_4_batch              l2-new  avgt    5   46,069 ±  3,191  ns/op
// computeDistance_native_segment_4_batch                  ip  avgt    5   58,537 ±  0,655  ns/op
// computeDistance_native_segment_4_batch              ip-new  avgt    5   50,224 ±  3,479  ns/op


@SuppressWarnings("unused")
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
//@Fork(value = 1, warmups = 1) // uncomment it to make things faster
public class DistanceComputationBenchmark {
    private static final int vectorSize = 128;

    @SuppressWarnings("FieldCanBeLocal")
    private final int numVectors = 4;

    private float[] qArr;
    private float[] v1Arr;
    private float[][] vectorsArr;
    private float[] resultArr;

    private Arena arena;

    private MemorySegment qSeg;
    private MemorySegment v1Seg;
    private MemorySegment[] vectorsSeg;

    @Param({
            "l2-cur-j",
            "l2-new-k",
            "l2-new-j",

            "ip-cur-j",
            "ip-new-k",
            "ip-new-j"
    })
    public String distanceFunction;

    private final Map<String, DistanceFunction> distanceFunctions = Map.of(
            "l2-cur-j", new L2DistanceFunction(),
            "l2-new-k", new L2DistanceFunctionNew(),
            "l2-new-j", new L2DistanceFunctionNewJ(),

            "ip-cur-j", new DotDistanceFunction(),
            "ip-new-k", new DotDistanceFunctionNew(),
            "ip-new-j", new DotDistanceFunctionNewJ()
    );

    private DistanceFunction distanceFun;

    @Setup(Level.Iteration)
    public void init() {
        var rnd = ThreadLocalRandom.current();
        arena = Arena.ofShared();

        qArr = new float[vectorSize];
        vectorsArr = new float[numVectors][];
        resultArr = new float[numVectors];

        qSeg = arena.allocate(vectorSize * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
        vectorsSeg = new MemorySegment[numVectors];

        for (int i = 0; i < numVectors; i++) {
            vectorsArr[i] = new float[vectorSize];
            vectorsSeg[i] = arena.allocate(vectorSize * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
        }

        for (int dimensionIdx = 0; dimensionIdx < vectorSize; dimensionIdx++) {
            qArr[dimensionIdx] = rnd.nextFloat();
            qSeg.setAtIndex(ValueLayout.JAVA_FLOAT, dimensionIdx, rnd.nextFloat());

            for (int vectorIdx = 0; vectorIdx < numVectors; vectorIdx++) {
                vectorsArr[vectorIdx][dimensionIdx] = rnd.nextFloat();
                vectorsSeg[vectorIdx].setAtIndex(ValueLayout.JAVA_FLOAT, dimensionIdx, rnd.nextFloat());
            }
        }
        v1Arr = vectorsArr[0];
        v1Seg = vectorsSeg[0];

        distanceFun = distanceFunctions.get(distanceFunction);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        arena.close();
        arena = null;
    }

    @Benchmark
    public float computeDistance_heap_array_1() {
        return distanceFun.computeDistance(qArr, 0, v1Arr, 0, vectorSize);
    }

    @Benchmark
    public float computeDistance_heap_array_4() {
        var res = distanceFun.computeDistance(qArr, 0, vectorsArr[0], 0, vectorSize);
        res += distanceFun.computeDistance(qArr, 0, vectorsArr[1], 0, vectorSize);
        res += distanceFun.computeDistance(qArr, 0, vectorsArr[2], 0, vectorSize);
        res += distanceFun.computeDistance(qArr, 0, vectorsArr[3], 0, vectorSize);
        return res;
    }

    @Benchmark
    public void computeDistance_heap_array_4_batch() {
        distanceFun.computeDistance(
                qArr, 0,
                vectorsArr[0], 0,
                vectorsArr[1], 0,
                vectorsArr[2], 0,
                vectorsArr[3], 0,
                resultArr, vectorSize
        );
    }

    @Benchmark
    public float computeDistance_native_segment_1() {
        return distanceFun.computeDistance(qSeg, 0, v1Seg, 0, vectorSize);
    }
    
    @Benchmark
    public float computeDistance_native_segment_4() {
        var res = distanceFun.computeDistance(qSeg, 0, vectorsSeg[0], 0, vectorSize);
        res += distanceFun.computeDistance(qSeg, 0, vectorsSeg[1], 0, vectorSize);
        res += distanceFun.computeDistance(qSeg, 0, vectorsSeg[2], 0, vectorSize);
        res += distanceFun.computeDistance(qSeg, 0, vectorsSeg[3], 0, vectorSize);
        return res;
    }

    @Benchmark
    public void computeDistance_native_segment_4_batch() {
        distanceFun.computeDistance(
                qSeg, 0,
                vectorsSeg[0], 0,
                vectorsSeg[1], 0,
                vectorsSeg[2], 0,
                vectorsSeg[3], 0,
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
