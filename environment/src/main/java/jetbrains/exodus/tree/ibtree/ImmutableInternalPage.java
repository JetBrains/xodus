package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.util.ArrayBackedByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;

/**
 * Presentation of immutable internal page in BTree.
 *
 * @see ImmutableBasePage
 */
final class ImmutableInternalPage extends ImmutableBasePage {
    @NotNull
    final ArrayBackedByteIterable currentPage;

    @NotNull
    final ImmutableBTree tree;

    ImmutableInternalPage(@NotNull ImmutableBTree tree, @NotNull Log log, @NotNull ArrayBackedByteIterable page,
                          long pageAddress) {
        super(log, page.subIterable(Long.BYTES, page.getLength() - Long.BYTES), pageAddress);
        this.tree = tree;

        currentPage = page;
    }

    @Override
    long getTreeSize() {
        return currentPage.getNativeLong(0);
    }

    @Override
    MutablePage toMutable(MutableBTree tree, ExpiredLoggableCollection expiredLoggables) {
        return new MutableInternalPage(tree, this, expiredLoggables, log);
    }

    @Override
    public ImmutableBasePage child(int index) {
        final int childAddressIndex = getChildAddressPositionIndex(index);

        final long childAddress = page.getNativeLong(childAddressIndex);
        return tree.loadPage(childAddress);
    }

    @Override
    public boolean isInternalPage() {
        return true;
    }

    @Override
    public ByteIterable value(int index) {
        throw new UnsupportedOperationException("Internal page does not contain values.");
    }
}
