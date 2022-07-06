package jetbrains.exodus.tree.ibtree;

import java.nio.ByteBuffer;
import java.util.Comparator;

final class ByteBufferComparator implements Comparator<ByteBuffer> {
    static final ByteBufferComparator INSTANCE = new ByteBufferComparator();

    @Override
    public int compare(ByteBuffer bufferOne, ByteBuffer bufferTwo) {
        int thisPos = bufferOne.position();
        int thisRem = bufferOne.limit() - thisPos;
        int thatPos = bufferTwo.position();
        int thatRem = bufferTwo.limit() - thatPos;
        int length = Math.min(thisRem, thatRem);

        if (length < 0) {
            return -1;
        } else {
            int i = bufferOne.mismatch(bufferTwo);
            if (i >= 0) {
                return Byte.compareUnsigned(bufferOne.get(thisPos + i), bufferTwo.get(thatPos + i));
            }

            return thisRem - thatRem;
        }
    }
}
