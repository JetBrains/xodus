package jetbrains.exodus.diskann.bench;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.*;
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
    public float baseLine() {
        return 0.0f;
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
        var sumVector = FloatVector.zero(species);

        var loopBound = species.loopBound(vector1.length);
        var step = species.length();

        for ( var index = 0; index < loopBound; index += step) {
            var first = FloatVector.fromArray(species, vector1, index);
            var second = FloatVector.fromArray(species, vector2, index);

            var diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistanceVector4Accumulators() {
        var sumVector_1 = FloatVector.zero(species);
        var sumVector_2 = FloatVector.zero(species);
        var sumVector_3 = FloatVector.zero(species);
        var sumVector_4 = FloatVector.zero(species);

        var loopBound = species.loopBound(vector1.length);
        var step = species.length();
        for (var index = 0; index < loopBound;) {
            var v_1_1 = FloatVector.fromArray(species, vector1, index);
            var v_2_1 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_2 = FloatVector.fromArray(species, vector1, index);
            var v_2_2 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_3 = FloatVector.fromArray(species, vector1, index);
            var v_2_3 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_4 = FloatVector.fromArray(species, vector1, index);
            var v_2_4 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var diff_1 = v_1_1.sub(v_2_1);
            var diff_2 = v_1_2.sub(v_2_2);
            var diff_3 = v_1_3.sub(v_2_3);
            var diff_4 = v_1_4.sub(v_2_4);

            sumVector_1 = diff_1.fma(diff_1, sumVector_1);
            sumVector_2 = diff_2.fma(diff_2, sumVector_2);
            sumVector_3 = diff_3.fma(diff_3, sumVector_3);
            sumVector_4 = diff_4.fma(diff_4, sumVector_4);
        }

        return sumVector_1.add(sumVector_2).add(sumVector_3).add(sumVector_4).reduceLanes(VectorOperators.ADD);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public float computeL2DistanceVector8Accumulators() {
        var sumVector_1 = FloatVector.zero(species);
        var sumVector_2 = FloatVector.zero(species);
        var sumVector_3 = FloatVector.zero(species);
        var sumVector_4 = FloatVector.zero(species);
        var sumVector_5 = FloatVector.zero(species);
        var sumVector_6 = FloatVector.zero(species);
        var sumVector_7 = FloatVector.zero(species);
        var sumVector_8 = FloatVector.zero(species);

        var loopBound = species.loopBound(vector1.length);
        var step = species.length();


        for (var index = 0; index < loopBound;) {
            var v_1_1 = FloatVector.fromArray(species, vector1, index);
            var v_2_1 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_2 = FloatVector.fromArray(species, vector1, index);
            var v_2_2 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_3 = FloatVector.fromArray(species, vector1, index);
            var v_2_3 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_4 = FloatVector.fromArray(species, vector1, index);
            var v_2_4 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_5 = FloatVector.fromArray(species, vector1, index);
            var v_2_5 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_6 = FloatVector.fromArray(species, vector1, index);
            var v_2_6 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_7 = FloatVector.fromArray(species, vector1, index);
            var v_2_7 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var v_1_8 = FloatVector.fromArray(species, vector1, index);
            var v_2_8 = FloatVector.fromArray(species, vector2, index);

            index += step;

            var diff_1 = v_1_1.sub(v_2_1);
            var diff_2 = v_1_2.sub(v_2_2);
            var diff_3 = v_1_3.sub(v_2_3);
            var diff_4 = v_1_4.sub(v_2_4);
            var diff_5 = v_1_5.sub(v_2_5);
            var diff_6 = v_1_6.sub(v_2_6);
            var diff_7 = v_1_7.sub(v_2_7);
            var diff_8 = v_1_8.sub(v_2_8);

            sumVector_1 = diff_1.fma(diff_1, sumVector_1);
            sumVector_2 = diff_2.fma(diff_2, sumVector_2);
            sumVector_3 = diff_3.fma(diff_3, sumVector_3);
            sumVector_4 = diff_4.fma(diff_4, sumVector_4);
            sumVector_5 = diff_5.fma(diff_5, sumVector_5);
            sumVector_6 = diff_6.fma(diff_6, sumVector_6);
            sumVector_7 = diff_7.fma(diff_7, sumVector_7);
            sumVector_8 = diff_8.fma(diff_8, sumVector_8);
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
