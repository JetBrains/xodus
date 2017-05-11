package jetbrains.exodus.benchmark.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLong23TreeSet;
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongSet;
import org.openjdk.jmh.annotations.*;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JMHPersistentLongSetBenchmark {
    private static final int MAP_SIZE = 100000;

    private final PersistentLongSet treeSet = new PersistentLong23TreeSet();
    private final PersistentLongSet bitTreeSet = new PersistentBitTreeLongSet();

    private final TreeMap<Long, Object> juTree = new TreeMap<>();
    private final Object value = new Object();

    private long existingKey = 0;
    private long missingKey = MAP_SIZE;

    @Setup
    public void prepare() {
        final PersistentLongSet.MutableSet mutableMap = treeSet.beginWrite();
        for (int i = 0; i < MAP_SIZE; ++i) {
            // the keys are even
            mutableMap.add((long) (i * 2));
            juTree.put((long) (i * 2), value);
        }
        mutableMap.endWrite();
    }

    @Setup(Level.Invocation)
    public void prepareKeys() {
        // the even key exists in the map, the odd one doesn't
        existingKey = (long) ((Math.random() * MAP_SIZE) * 2);
        missingKey = existingKey + 1;
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public boolean get23TreeExisting() {
        return treeSet.beginRead().contains(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public boolean get23TreeMissing() {
        return treeSet.beginRead().contains(missingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public boolean getBitTreeExisting() {
        return bitTreeSet.beginRead().contains(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public boolean getBitTreeMissing() {
        return bitTreeSet.beginRead().contains(missingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public boolean treeMapGetExisting() {
        return juTree.containsKey(existingKey);
    }

    @Benchmark
    @Warmup(iterations = 4, time = 1)
    @Measurement(iterations = 6, time = 1)
    @Fork(5)
    public boolean treeMapGetMissing() {
        return juTree.containsKey(missingKey);
    }
}
