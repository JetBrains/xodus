package jetbrains.vectoriadb.index.segment;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class IntSegment {

    private static final ValueLayout.OfInt LAYOUT = ValueLayout.JAVA_INT;
    private static final int BYTES = Integer.BYTES;

    private final int count;

    @NotNull
    private final MemorySegment segment;

    public IntSegment(int count, @NotNull MemorySegment segment) {
        this.count = count;
        this.segment = segment;
    }

    public int count() { return count; }

    public void fill(byte value) {
        segment.fill(value);
    }

    public int get(int idx) {
        return segment.getAtIndex(LAYOUT, idx);
    }

    public void set(int idx, int value) {
        segment.setAtIndex(LAYOUT, idx, value);
    }

    public void add(int idx, int value) {
        var currentValue = segment.getAtIndex(LAYOUT, idx);
        segment.setAtIndex(LAYOUT, idx, currentValue + value);
    }

    public static IntSegment makeNativeSegment(Arena arena, int count) {
        var segment = arena.allocate((long) count * BYTES, LAYOUT.byteAlignment());
        return new IntSegment(count, segment);
    }

    public static IntSegment[] makeNativeSegments(Arena arena, int segmentCount, int itemsPerSegment) {
        var segment = arena.allocate((long) segmentCount * itemsPerSegment * BYTES, LAYOUT.byteAlignment());
        var result = new IntSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            result[i] = new IntSegment(itemsPerSegment, segment.asSlice((long) i * itemsPerSegment * BYTES, (long) itemsPerSegment * BYTES));
        }
        return result;
    }
}
