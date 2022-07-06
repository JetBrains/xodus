package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;

abstract class ImmutableBasePage {
    static final int PAGES_OFFSET = 0;
    static final int PAGES_SIZE = Short.BYTES;

    static final int KEY_PREFIX_LEN_OFFSET = PAGES_OFFSET + PAGES_SIZE;
    static final int KEY_PREFIX_LEN_SIZE = Short.BYTES;

    static final int ENTRIES_OFFSET = KEY_PREFIX_LEN_OFFSET + KEY_PREFIX_LEN_SIZE;
    static final int ENTRIES_SIZE = Integer.BYTES;

    static final int KEYS_OFFSET = ENTRIES_OFFSET + ENTRIES_SIZE;

    static final int KEY_SIZE_SIZE = Integer.BYTES;
    static final int KEY_POSITION_SIZE = Integer.BYTES;
    static final int KEY_ENTRY_SIZE = KEY_SIZE_SIZE + KEY_POSITION_SIZE;

    @NotNull
    final Log log;

    @NotNull
    final ByteBuffer page;
    final int pageSize;
    final long pageIndex;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @NotNull
    final List<ByteBuffer> keyView;

    final int basicOffset;

    protected ImmutableBasePage(Log log, int pageSize, long pageIndex, int basicOffset) {
        this.log = log;
        this.pageSize = pageSize;
        this.pageIndex = pageIndex;
        this.basicOffset = basicOffset;

        page = log.readPage(pageIndex);

        //ensure that allocated page aligned to ensure fastest memory access and stable offsets of the data
        assert page.alignmentOffset(bufferPosition(0), Long.BYTES) == 0;
        assert page.order() == ByteOrder.nativeOrder();

        keyView = new KeyView();
    }

    final ByteBuffer fetchByteChunk(final int position, final int size) {
        //byte chunk belongs to the first page
        if (bufferPosition(position + size) <= page.limit()) {
            return page.slice(bufferPosition(position), size).order(ByteOrder.nativeOrder());
        }

        int startPositionIndex = position / pageSize;
        int endPositionIndex = (position + size) / pageSize;

        //byte chunk stored in single page
        if (startPositionIndex == endPositionIndex) {
            final ByteBuffer loadedPage = log.readPage(pageIndex + startPositionIndex);
            final int offset = startPositionIndex * pageSize;
            return loadedPage.slice(position - offset, size).order(ByteOrder.nativeOrder());
        }

        final ByteBuffer buffer = ByteBuffer.allocate(size);

        //offset to the position to the record relatively to the loaded page
        int pageOffset = position - (position / pageSize) * pageSize;

        int index = position / pageSize;

        while (buffer.remaining() > 0) {
            final ByteBuffer loadedPage = log.readPage(pageIndex + index);

            loadedPage.position(pageOffset);
            loadedPage.limit(Math.min(buffer.remaining(), loadedPage.remaining()));
            loadedPage.put(buffer);

            pageOffset = 0;
        }

        return buffer.rewind().order(ByteOrder.nativeOrder());
    }

    final int find(ByteBuffer key) {
        return Collections.binarySearch(keyView, key, ByteBufferComparator.INSTANCE);
    }

    final int bufferPosition(int offset) {
        return basicOffset + offset;
    }

    private ByteBuffer getKey(int index) {
        final int keyPosition = getKeyPosition(index);
        final int ketSize = getKeySize(index);

        return fetchByteChunk(keyPosition, ketSize);
    }

    final int getEntries() {
        assert page.alignmentOffset(bufferPosition(ENTRIES_OFFSET), Integer.BYTES) == 0;

        return page.getInt(bufferPosition(ENTRIES_OFFSET));
    }

    final int getPages() {
        assert page.alignmentOffset(bufferPosition(PAGES_OFFSET), Short.BYTES) == 0;

        return page.getShort(bufferPosition(PAGES_OFFSET));
    }

    private int getKeyPosition(int index) {
        final int position = bufferPosition(KEYS_OFFSET + index * KEY_ENTRY_SIZE);

        assert page.alignmentOffset(position, Integer.BYTES) == 0;

        return page.getInt(position);
    }

    private int getKeySize(int index) {
        final int position = bufferPosition(KEYS_OFFSET + index * KEY_ENTRY_SIZE + KEY_POSITION_SIZE);

        assert page.alignmentOffset(position, Integer.BYTES) == 0;

        return page.getInt(position);
    }

    final ByteBuffer key(int index) {
        return keyView.get(index);
    }

    final ByteBuffer getKeyUnsafe(int index) {
        return getKey(index);
    }

    final class KeyView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int i) {
            return getKey(i);
        }

        @Override
        public int size() {
            return getEntries();
        }
    }
}
