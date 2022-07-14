package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Presentation of immutable internal page in BTree.
 * Representation of common layout of all pages both leaf and internal.
 * Layout composed as following:
 * <ol>
 *     <li>Key prefix size. Size of the common prefix which was truncated for all keys in tree.
 *     Currently not used but added to implement key compression without breaking of binary compatibility.</li>
 *     <li>Count of the entries contained inside of the given page.</li>
 *     <li>Array each entry of which contains either address of the loggable which contains key of the entry or pair
 *     (key position, key size) where 'key position' is position of the key stored inside of this page,
 *     key size is accordingly size of this key. To distinguish between those two meanings of the same value,
 *     key addresses always negative and their sign should be changed to load key.</li>
 *     <li> Array each entry of which contains address of the mutableChild page.</li>
 *     <li>Array each entry of which contains treeSize of entries contained by subtree pointed related item of array
 *     described above.</li>
 *     <li>Array of keys embedded into this page.</li>
 * </ol>
 * <p>
 *
 * @see ImmutableBasePage
 */
final class ImmutableInternalPage extends ImmutableBasePage {
    @NotNull
    final ByteBuffer currentPage;

    ImmutableInternalPage(@NotNull Log log, @NotNull ByteBuffer page, long pageAddress) {
        super(log, page.slice(Long.BYTES, page.limit() - Long.BYTES), pageAddress);

        currentPage = page;
        assert currentPage.alignmentOffset(0, Long.BYTES) == 0;
    }

    @Override
    long getTreeSize() {
        assert currentPage.alignmentOffset(0, Long.BYTES) == 0;
        return page.getLong(0);
    }

    private int getSubTreeSizePosition(int index) {
        return KEYS_OFFSET + 2 * getEntriesCount() * Long.BYTES + index * Integer.BYTES;
    }

    int getSubTreeSize(int index) {
        int position = getSubTreeSizePosition(index);
        assert page.alignmentOffset(position, Integer.BYTES) == 0;

        return page.getInt(position);
    }
}
