package jetbrains.exodus.diskann.bench;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class L2DistanceBench {
    private static final int VECTOR_SIZE = 1024;
    private static final VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;

    private float[] vector1;
    private float[] vector2;

    @Setup(Level.Iteration)
    public void init() {
        var rnd = ThreadLocalRandom.current();
        vector1 = new float[VECTOR_SIZE];
        vector2 = new float[VECTOR_SIZE];

        for (int i = 0; i < VECTOR_SIZE; i++) {
            vector1[i] = rnd.nextFloat();
            vector2[i] = rnd.nextFloat();
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistancePlain() {
        float sum = 0;
        for (int i = 0; i < vector1.length; i++) {
            float diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }
        return sum;
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
    public float computeL2DistanceVector4Accumulators() {
        var step = species.length();

        var v_1 = FloatVector.fromArray(species, vector1, 0);
        var v_2 = FloatVector.fromArray(species, vector2, 0);
        var diff = v_1.sub(v_2);
        var sumVector_1 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, step);
        v_2 = FloatVector.fromArray(species, vector2, step);
        diff = v_1.sub(v_2);
        var sumVector_2 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 2 * step);
        v_2 = FloatVector.fromArray(species, vector2, 2 * step);
        diff = v_1.sub(v_2);
        var sumVector_3 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 3 * step);
        v_2 = FloatVector.fromArray(species, vector2, 3 * step);
        diff = v_1.sub(v_2);
        var sumVector_4 = diff.mul(diff);

        var loopBound = species.loopBound(vector1.length);
        for (var index = 4 * step; index < loopBound; ) {
            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_1 = diff.fma(diff, sumVector_1);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_2 = diff.fma(diff, sumVector_2);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_3 = diff.fma(diff, sumVector_3);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_4 = diff.fma(diff, sumVector_4);

            index += step;
        }

        return sumVector_1.add(sumVector_2).add(sumVector_3).add(sumVector_4).reduceLanes(VectorOperators.ADD);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistanceVector8Accumulators() {
        var step = species.length();

        var v_1 = FloatVector.fromArray(species, vector1, 0);
        var v_2 = FloatVector.fromArray(species, vector2, 0);
        var diff = v_1.sub(v_2);
        var sumVector_1 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, step);
        v_2 = FloatVector.fromArray(species, vector2, step);
        diff = v_1.sub(v_2);
        var sumVector_2 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 2 * step);
        v_2 = FloatVector.fromArray(species, vector2, 2 * step);
        diff = v_1.sub(v_2);
        var sumVector_3 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 3 * step);
        v_2 = FloatVector.fromArray(species, vector2, 3 * step);
        diff = v_1.sub(v_2);
        var sumVector_4 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 4 * step);
        v_2 = FloatVector.fromArray(species, vector2, 4 * step);
        diff = v_1.sub(v_2);
        var sumVector_5 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 5 * step);
        v_2 = FloatVector.fromArray(species, vector2, 5 * step);
        diff = v_1.sub(v_2);
        var sumVector_6 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 6 * step);
        v_2 = FloatVector.fromArray(species, vector2, 6 * step);
        diff = v_1.sub(v_2);
        var sumVector_7 = diff.mul(diff);

        v_1 = FloatVector.fromArray(species, vector1, 7 * step);
        v_2 = FloatVector.fromArray(species, vector2, 7 * step);
        diff = v_1.sub(v_2);
        var sumVector_8 = diff.mul(diff);

        var loopBound = species.loopBound(vector1.length);
        for (var index = 8 * step; index < loopBound; ) {
            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);

            sumVector_1 = diff.fma(diff, sumVector_1);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_2 = diff.fma(diff, sumVector_2);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_3 = diff.fma(diff, sumVector_3);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_4 = diff.fma(diff, sumVector_4);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_5 = diff.fma(diff, sumVector_5);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_6 = diff.fma(diff, sumVector_6);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_7 = diff.fma(diff, sumVector_7);
            index += step;

            v_1 = FloatVector.fromArray(species, vector1, index);
            v_2 = FloatVector.fromArray(species, vector2, index);
            diff = v_1.sub(v_2);
            sumVector_8 = diff.fma(diff, sumVector_8);

            index += step;
        }

        return sumVector_1.add(sumVector_2).add(sumVector_3).add(sumVector_4).
                add(sumVector_5).add(sumVector_6).add(sumVector_7).add(sumVector_8).
                reduceLanes(VectorOperators.ADD);
    }


    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(L2DistanceBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

}
