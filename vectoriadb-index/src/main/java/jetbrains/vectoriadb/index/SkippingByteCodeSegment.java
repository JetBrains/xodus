package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

class SkippingByteCodeSegment implements CodeSegment {

    private final MemorySegment codes;
    private final int range;
    private final int idxInRange;

    SkippingByteCodeSegment(@NotNull MemorySegment codes, int range, int idxInRange) {
        this.codes = codes;
        this.range = range;
        this.idxInRange = idxInRange;
    }

    @Override
    public int count() {
        return (int) (codes.byteSize() / range);
    }

    @Override
    public int get(int vectorIdx) {
        return codes.getAtIndex(ValueLayout.JAVA_BYTE, calcIdx(vectorIdx)) & 0xFF;
    }

    @Override
    public void set(int vectorIdx, int value) {
        codes.setAtIndex(ValueLayout.JAVA_BYTE, calcIdx(vectorIdx), (byte) value);
    }

    @Override
    public int maxNumberOfCodes() {
        return 256;
    }

    private int calcIdx(int vectorIdx) {
        return vectorIdx * range + idxInRange;
    }
}
