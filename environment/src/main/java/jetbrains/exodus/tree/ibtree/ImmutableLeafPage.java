package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Presentation of immutable leaf page in BTree.
 *
 * @see ImmutableBasePage
 */
final class ImmutableLeafPage extends ImmutableBasePage {
    ImmutableLeafPage(@NotNull Log log, @NotNull ByteBuffer page, long pageAddress) {
        super(log, page, pageAddress);
    }

    @Override
    long getTreeSize() {
        return getEntriesCount();
    }

    @Override
    MutablePage toMutable(ExpiredLoggableCollection expiredLoggables, MutableInternalPage parent) {
        return new MutableLeafPage(this, log, log.getCachePageSize(), expiredLoggables, parent);
    }

    ByteBuffer getValue(final int index) {
        final long valueAddress = getChildAddress(index);
        var loggable = log.read(valueAddress);

        assert loggable.getType() == ImmutableBTree.VALUE_NODE;

        var data = loggable.getData();
        return data.getByteBuffer();
    }
}
