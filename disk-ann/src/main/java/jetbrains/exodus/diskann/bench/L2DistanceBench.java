package jetbrains.exodus.diskann.bench;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class L2DistanceBench {
    private static final int VECTOR_SIZE = 1024;
    private static final VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;

    private float[] vector1;
    private float[] vector2;
    private float[] vector3;
    private float[] vector4;
    private float[] vector5;

    @Setup(Level.Iteration)
    public void init() {
        var rnd = ThreadLocalRandom.current();
        vector1 = new float[VECTOR_SIZE];
        vector2 = new float[VECTOR_SIZE];
        vector3 = new float[VECTOR_SIZE];
        vector4 = new float[VECTOR_SIZE];
        vector5 = new float[VECTOR_SIZE];

        for (int i = 0; i < VECTOR_SIZE; i++) {
            vector1[i] = rnd.nextFloat();
            vector2[i] = rnd.nextFloat();
            vector3[i] = rnd.nextFloat();
            vector4[i] = rnd.nextFloat();
            vector5[i] = rnd.nextFloat();
        }

    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void computeL2DistanceVector(Blackhole bh) {
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

        bh.consume(sumVector.reduceLanes(VectorOperators.ADD));
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void computeL2DistanceVectorBatch(Blackhole bh) {
        var origin = FloatVector.fromArray(species, vector1, 0);

        var first = FloatVector.fromArray(species, vector2, 0);
        var second = FloatVector.fromArray(species, vector3, 0);
        var third = FloatVector.fromArray(species, vector4, 0);
        var fourth = FloatVector.fromArray(species, vector5, 0);

        var diff_1 = origin.sub(first);
        var diff_2 = origin.sub(second);
        var diff_3 = origin.sub(third);
        var diff_4 = origin.sub(fourth);

        var sumVector_1 = diff_1.mul(diff_1);
        var sumVector_2 = diff_2.mul(diff_2);
        var sumVector_3 = diff_3.mul(diff_3);
        var sumVector_4 = diff_4.mul(diff_4);

        var loopBound = species.loopBound(vector1.length);
        var step = species.length();

        for (var index = step; index < loopBound; index += step) {
            origin = FloatVector.fromArray(species, vector1, index);

            first = FloatVector.fromArray(species, vector2, index);
            second = FloatVector.fromArray(species, vector3, index);
            third = FloatVector.fromArray(species, vector4, index);
            fourth = FloatVector.fromArray(species, vector5, index);

            diff_1 = origin.sub(first);
            diff_2 = origin.sub(second);
            diff_3 = origin.sub(third);
            diff_4 = origin.sub(fourth);

            sumVector_1 = diff_1.fma(diff_1, sumVector_1);
            sumVector_2 = diff_2.fma(diff_2, sumVector_2);
            sumVector_3 = diff_3.fma(diff_3, sumVector_3);
            sumVector_4 = diff_4.fma(diff_4, sumVector_4);
        }


        bh.consume(sumVector_1.reduceLanes(VectorOperators.ADD));
        bh.consume(sumVector_2.reduceLanes(VectorOperators.ADD));
        bh.consume(sumVector_3.reduceLanes(VectorOperators.ADD));
        bh.consume(sumVector_4.reduceLanes(VectorOperators.ADD));
    }


    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(L2DistanceBench.class.getSimpleName()).addProfiler(LinuxPerfProfiler.class,
                        "events=fp_ret_sse_avx_ops.all;delay=1000")
                .build();
        new Runner(opt).run();
    }

}
