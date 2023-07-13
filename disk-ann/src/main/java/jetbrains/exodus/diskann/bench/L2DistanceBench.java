package jetbrains.exodus.diskann.bench;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import jetbrains.exodus.diskann.DiskANN;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class L2DistanceBench {
    private static final int VECTOR_SIZE = 1024;
    private static final VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;

    private float[] vector1;
    private float[] vector2;

    private MemorySegment segment1;
    private MemorySegment segment2;

    private Arena arena;

    @Setup(Level.Iteration)
    public void init() {
        var rnd = ThreadLocalRandom.current();
        vector1 = new float[VECTOR_SIZE];
        vector2 = new float[VECTOR_SIZE];

        for (int i = 0; i < VECTOR_SIZE; i++) {
            vector1[i] = rnd.nextFloat();
            vector2[i] = rnd.nextFloat();
        }

        arena = Arena.openShared();

        segment1 = arena.allocate(Float.BYTES * VECTOR_SIZE, Float.BYTES);
        segment2 = arena.allocate(Float.BYTES * VECTOR_SIZE, Float.BYTES);

        for (int i = 0; i < VECTOR_SIZE; i++) {
            segment1.setAtIndex(ValueLayout.JAVA_FLOAT, i, vector1[i]);
            segment2.setAtIndex(ValueLayout.JAVA_FLOAT, i, vector2[i]);
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        arena.close();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistanceVector() {
        var first = FloatVector.fromArray(species, vector1, 0);
        var second = FloatVector.fromArray(species, vector2, 0);
        var diff = first.sub(second);

        var sumVector = diff.mul(diff);

        var loopBound = species.loopBound(vector1.length);
        var step = species.length();

        for (var index = step; index < loopBound; index += step) {
            first = FloatVector.fromArray(species, vector1, index);
            second = FloatVector.fromArray(species, vector2, index);

            diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistanceVectorMemorySegment() {
        var first = FloatVector.fromMemorySegment(species, segment1, 0, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(species, segment2, 0, ByteOrder.nativeOrder());
        var diff = first.sub(second);

        var sumVector = diff.mul(diff);

        var loopBound = species.loopBound(VECTOR_SIZE);
        var step = species.length();
        var segmentStep = step * Float.BYTES;
        var segmentOffset = segmentStep;

        for (var index = step; index < loopBound; index += step, segmentOffset += segmentStep) {
            first = FloatVector.fromMemorySegment(species, segment1, segmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(species, segment2, segmentOffset, ByteOrder.nativeOrder());

            diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistanceVectorMemorySegmentDiskANNVector() {
        return DiskANN.computeL2Distance(vector1, vector2, 0);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistanceVectorMemorySegmentDiskANNSegment() {
        return DiskANN.computeL2Distance(segment1, 0, segment2, 0, VECTOR_SIZE);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(L2DistanceBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

}
