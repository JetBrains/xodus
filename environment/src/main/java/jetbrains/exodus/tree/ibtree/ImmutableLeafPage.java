package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.util.ArrayBackedByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;

/**
 * Presentation of immutable leaf page in BTree.
 *
 * @see ImmutableBasePage
 */
final class ImmutableLeafPage extends ImmutableBasePage {
    ImmutableLeafPage(@NotNull Log log, @NotNull ArrayBackedByteIterable page, long pageAddress) {
        super(log, page, pageAddress);
    }

    @Override
    long getTreeSize() {
        return getEntriesCount();
    }

    @Override
    public MutablePage toMutable(MutableBTree tree, ExpiredLoggableCollection expiredLoggables) {
        return new MutableLeafPage(tree, this, log, expiredLoggables);
    }

    @Override
    public TraversablePage child(int index) {
        throw new UnsupportedOperationException("Leaf page does not contain child pages");
    }

    @Override
    public boolean isInternalPage() {
        return false;
    }

    public ArrayBackedByteIterable value(final int index) {
        final int valuePositionSizeIndex = getChildAddressPositionIndex(index);

        return extractByteChunk(valuePositionSizeIndex);
    }


}
