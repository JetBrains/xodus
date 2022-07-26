package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Presentation of immutable internal page in BTree.
 *
 * @see ImmutableBasePage
 */
final class ImmutableInternalPage extends ImmutableBasePage {
    @NotNull
    final ByteBuffer currentPage;

    @NotNull
    final ImmutableBTree tree;

    ImmutableInternalPage(@NotNull ImmutableBTree tree, @NotNull Log log, @NotNull ByteBuffer page, long pageAddress) {
        super(log, page.slice(Long.BYTES, page.limit() - Long.BYTES).order(ByteOrder.nativeOrder()), pageAddress);
        this.tree = tree;

        currentPage = page;
        assert currentPage.alignmentOffset(0, Long.BYTES) == 0;
    }

    @Override
    long getTreeSize() {
        assert currentPage.alignmentOffset(0, Long.BYTES) == 0;
        return page.getLong(0);
    }

    @Override
    MutablePage toMutable(MutableBTree tree, ExpiredLoggableCollection expiredLoggables, MutableInternalPage parent) {
        return new MutableInternalPage(tree, this, expiredLoggables, log, log.getCachePageSize(), parent);
    }

    private int getSubTreeSizePosition(int index) {
        return KEYS_OFFSET + 2 * getEntriesCount() * Long.BYTES + index * Integer.BYTES;
    }

    int getSubTreeSize(int index) {
        int position = getSubTreeSizePosition(index);
        assert page.alignmentOffset(position, Integer.BYTES) == 0;

        return page.getInt(position);
    }

    @Override
    public ImmutableBasePage child(int index) {
        final int childAddressIndex = getChildAddressPositionIndex(index);

        assert page.alignmentOffset(childAddressIndex, Long.BYTES) == 0;
        final long childAddress = page.getLong(childAddressIndex);

        return tree.loadPage(childAddress);
    }

    @Override
    public boolean isInternalPage() {
        return true;
    }

    @Override
    public ByteBuffer value(int index) {
        throw new UnsupportedOperationException("Internal page does not contain values.");
    }
}
