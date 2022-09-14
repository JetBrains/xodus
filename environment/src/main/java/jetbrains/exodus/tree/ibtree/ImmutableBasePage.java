package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.util.ArrayBackedByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;

/**
 * Representation of common layout of all pages both leaf and internal.
 * Layout composed as following:
 * <ol>
 *     <li>Count of the entries contained inside of the given page</li>
 *     <li>Key prefix size</li>
 *     <li>Array each entry of which contains pair (key position, key size) where 'key position' is position
 *     of the key stored inside of this page,  key size is accordingly size of this key.</li>
 *     <li> Array each entry of which contains either (value position, value size) pair if that is
 *     {@link  ImmutableLeafPage} or address of the mutableChild page if that is
 *     {@link ImmutableInternalPage}</li>
 *     <li>Key prefix, if size is not 0</li>
 *     <li>Array of keys</li>
 *     <li>Array of values. Only for {@link  ImmutableLeafPage}. Aligned from the end of the page.</li>
 * </ol>
 * <p>
 * <p>
 * Internal pages also keep size of the whole (sub)tree at the header of the page.
 */
abstract class ImmutableBasePage implements TraversablePage {
    static final int ENTRIES_COUNT_OFFSET = 0;
    static final int ENTRIES_COUNT_SIZE = Integer.BYTES;

    static final int KEY_PREFIX_LEN_OFFSET = ENTRIES_COUNT_OFFSET + ENTRIES_COUNT_SIZE;
    static final int KEY_PREFIX_LEN_SIZE = Integer.BYTES;

    static final int ENTRY_POSITIONS_OFFSET = KEY_PREFIX_LEN_OFFSET + KEY_PREFIX_LEN_SIZE;

    @NotNull
    final Log log;
    final long address;

    @NotNull
    final ArrayBackedByteIterable page;

    final int entriesCount;
    final int keyPrefixSize;
    final ArrayBackedByteIterable keyPrefix;

    protected ImmutableBasePage(@NotNull final Log log, @NotNull final ArrayBackedByteIterable page, long address) {
        this.log = log;
        this.address = address;
        this.page = page;

        entriesCount = page.getNativeInt(ENTRIES_COUNT_OFFSET);
        keyPrefixSize = page.getNativeInt(KEY_PREFIX_LEN_OFFSET);

        if (keyPrefixSize > 0) {
            keyPrefix = page.subIterable(ENTRY_POSITIONS_OFFSET + 2 * Long.BYTES * entriesCount,
                    keyPrefixSize);
        } else {
            keyPrefix = null;
        }
    }

    @Override
    public int getKeyPrefixSize() {
        return keyPrefixSize;
    }

    abstract long getTreeSize();

    public final int find(ByteIterable key) {
        var offset = page.offset;
        var limit = page.limit;

        final int position = binarySearch(key, offset, limit);

        page.offset = offset;
        page.limit = limit;

        return position;
    }

    private int binarySearch(ByteIterable key, int basicOffset, int basiLimit) {
        int low = 0;
        int high = entriesCount - 1;

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final int position = ENTRY_POSITIONS_OFFSET + mid * Long.BYTES;

            var offset = page.getNativeInt(position);

            page.limit = basicOffset + offset + page.getNativeInt(position + Integer.BYTES);
            page.offset = basicOffset + offset;

            assert page.offset <= page.limit;

            final int cmp = page.compareTo(key);

            page.limit = basiLimit;
            page.offset = basicOffset;

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }

        return -(low + 1);  // key not found
    }

    @Override
    public ArrayBackedByteIterable keyPrefix() {
        return keyPrefix;
    }

    private int getKeyPositionSizeIndex(final int index) {
        return ENTRY_POSITIONS_OFFSET + index * Long.BYTES;
    }

    private ArrayBackedByteIterable getKey(int index) {
        final int ketPositionSizeIndex = getKeyPositionSizeIndex(index);
        return extractByteChunk(ketPositionSizeIndex);
    }

    final ArrayBackedByteIterable extractByteChunk(int valuePositionSizeIndex) {
        final int valuePosition = page.getNativeInt(valuePositionSizeIndex);
        final int valueSize = page.getNativeInt(valuePositionSizeIndex + Integer.BYTES);

        return page.subIterable(valuePosition, valueSize);
    }

    final int getChildAddressPositionIndex(int index) {
        return ENTRY_POSITIONS_OFFSET + getEntriesCount() * Long.BYTES + index * Long.BYTES;
    }

    public final int getEntriesCount() {
        return entriesCount;
    }

    public final ArrayBackedByteIterable key(int index) {
        return getKey(index);
    }

    abstract MutablePage toMutable(MutableBTree tree, ExpiredLoggableCollection expiredLoggables);

    @Override
    public final long address() {
        return address;
    }
}
