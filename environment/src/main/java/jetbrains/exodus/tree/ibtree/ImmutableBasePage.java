package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Collections;
import java.util.RandomAccess;

/**
 * Representation of common layout of all pages both leaf and internal.
 * Layout composed as following:
 * <ol>
 *     <li>Key prefix size. Size of the common prefix which was truncated for all keys in tree.
 *     Currently not used but added to implement key compression without breaking of binary compatibility</li>
 *     <li>Count of the entries contained inside of the given page</li>
 *     <li>Array each entry of which contains pair (key position, key size) where 'key position' is position
 *     of the key stored inside of this page,  key size is accordingly size of this key.</li>
 *     <li> Array each entry of which contains either (value position, value size) pair if that is
 *     {@link  ImmutableLeafPage} or address of the mutableChild page if that is
 *     {@link ImmutableInternalPage}</li>
 *     <li>Array of keys for {@link ImmutableLeafPage}</li>
 *     <li>Array of values (only for {@link ImmutableLeafPage})</li>
 * </ol>
 * <p>
 * <p>
 * Internal pages also keep size of the whole (sub)tree at the header of the page.
 */
abstract class ImmutableBasePage implements TraversablePage {
    static final int KEY_PREFIX_LEN_OFFSET = 0;

    //we could use short here but in such case we risk to get unaligned memory access for subsequent reads
    //so we use int
    static final int KEY_PREFIX_LEN_SIZE = Integer.BYTES;

    static final int ENTRIES_COUNT_OFFSET = KEY_PREFIX_LEN_OFFSET + KEY_PREFIX_LEN_SIZE;
    static final int ENTRIES_COUNT_SIZE = Integer.BYTES;

    static final int KEYS_OFFSET = ENTRIES_COUNT_OFFSET + ENTRIES_COUNT_SIZE;

    @NotNull
    final Log log;
    final long address;

    @NotNull
    final ByteBuffer page;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @NotNull
    final KeyView keyView;

    @NotNull
    final NonCopyKeyView nonCopyKeyView;

    final int entriesCount;
    final int keyPrefixSize;

    protected ImmutableBasePage(@NotNull final Log log, @NotNull final ByteBuffer page, long address) {
        this.log = log;
        this.address = address;
        this.page = page;

        //ensure that allocated page aligned to ensure fastest memory access and stable offsets of the data
        assert page.alignmentOffset(0, Long.BYTES) == 0;
        assert page.order() == ByteOrder.nativeOrder();

        keyView = new KeyView();
        nonCopyKeyView = new NonCopyKeyView();

        assert page.alignmentOffset(ENTRIES_COUNT_OFFSET, Integer.BYTES) == 0;
        entriesCount = page.getInt(ENTRIES_COUNT_OFFSET);

        assert page.alignmentOffset(KEY_PREFIX_LEN_OFFSET, Integer.BYTES) == 0;
        keyPrefixSize = page.getInt(KEY_PREFIX_LEN_OFFSET);
    }

    @Override
    public int getKeyPrefixSize() {
        return keyPrefixSize;
    }

    abstract long getTreeSize();

    public final int find(ByteBuffer key) {
        final int position = Collections.binarySearch(nonCopyKeyView, key, ByteBufferComparator.INSTANCE);
        page.clear();
        return position;

    }

    private int getKeyPositionSizeIndex(final int index) {
        return KEYS_OFFSET + index * Long.BYTES;
    }

    private ByteBuffer getKey(int index) {
        final int ketPositionSizeIndex = getKeyPositionSizeIndex(index);
        return extractByteChunk(ketPositionSizeIndex);
    }

    private ByteBuffer getKeyNoCopy(int index) {
        final int ketPositionSizeIndex = getKeyPositionSizeIndex(index);
        return extractByteChunkNoCopy(ketPositionSizeIndex);
    }

    final ByteBuffer extractByteChunk(int valuePositionSizeIndex) {
        assert page.alignmentOffset(valuePositionSizeIndex, Integer.BYTES) == 0;
        final int valuePosition = page.getInt(valuePositionSizeIndex);

        assert page.alignmentOffset(valuePositionSizeIndex + Integer.BYTES, Integer.BYTES) == 0;
        final int valueSize = page.getInt(valuePositionSizeIndex + Integer.BYTES);

        return page.slice(valuePosition, valueSize);
    }

    private ByteBuffer extractByteChunkNoCopy(int valuePositionSizeIndex) {
        assert page.alignmentOffset(valuePositionSizeIndex, Integer.BYTES) == 0;
        final int valuePosition = page.getInt(valuePositionSizeIndex);

        assert page.alignmentOffset(valuePositionSizeIndex + Integer.BYTES, Integer.BYTES) == 0;
        final int valueSize = page.getInt(valuePositionSizeIndex + Integer.BYTES);

        return page.limit(valuePosition + valueSize).position(valuePosition);
    }

    final int getChildAddressPositionIndex(int index) {
        return KEYS_OFFSET + getEntriesCount() * Long.BYTES + index * Long.BYTES;
    }

    public final int getEntriesCount() {
        return entriesCount;
    }

    public final ByteBuffer key(int index) {
        return keyView.get(index);
    }

    abstract MutablePage toMutable(MutableBTree tree, ExpiredLoggableCollection expiredLoggables);

    private final class KeyView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int i) {
            return getKey(i);
        }

        @Override
        public int size() {
            return entriesCount;
        }
    }

    private final class NonCopyKeyView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int index) {
            return getKeyNoCopy(index);
        }

        @Override
        public int size() {
            return entriesCount;
        }
    }

    @Override
    public final long address() {
        return address;
    }
}
