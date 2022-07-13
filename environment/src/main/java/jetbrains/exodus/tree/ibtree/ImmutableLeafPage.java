package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ByteBufferIterable;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;

/**
 * Presentation of immutable leaf page in BTree.
 *
 * @see ImmutableBasePage
 */
final class ImmutableLeafPage extends ImmutableBasePage {
    ImmutableLeafPage(Log log, int pageSize, long pageAddress, int pageOffset) {
        super(log, pageSize, pageAddress, pageOffset);
    }

    ByteBuffer getValue(final int index) {
        final long valueAddress = getChildAddress(index);
        var loggable = log.read(valueAddress);

        assert loggable.getType() == BTreeBase.VALUE;

        var data = loggable.getData();
        return data.getByteBuffer();
    }
}
