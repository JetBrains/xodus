package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;

/**
 * Presentation of immutable leaf page in BTree.
 *
 * <p>
 * Page consist of the following fields:
 * <ol>
 *     <li>Key prefix size. Size of the common prefix which was truncated for all keys in tree.
 *     Currently not used but added to implement key compression without breaking of binary compatibility.</li>
 *     <li>Amount of entries page contains</li>
 *     <li>Array each entry of which contains:
 *      <ol>
 *         <li>Key position relative to the start of the page</li>
 *         <li>Key length</li>
 *      </ol>
 *     </li>
 *     <li> Array each entry of which contains:
 *       <ol>
 *          <li>Value position relative to the start of the page</li>
 *          <li>Value length</li>
 *       </ol>
 *     </li>
 *     <li>Array of key-value pairs</li>
 * </ol>
 * <p>
 * <p>
 * Each page is made to be big enough to store at least one key-value entry
 * Initially only one page is loaded but if that is not enough, because key-value entry is too big to be stored
 * inside of the single page additional pages will be loaded from the log.
 */
final class ImmutableLeafPage extends ImmutableBasePage {
    static final int VALUE_SIZE_SIZE = Integer.BYTES;
    static final int VALUE_POSITION_SIZE = Integer.BYTES;
    static final int VALUE_ENTRY_SIZE = VALUE_SIZE_SIZE + VALUE_POSITION_SIZE;


    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @NotNull
    final List<ByteBuffer> valueView;

    ImmutableLeafPage(Log log, int pageSize, long pageIndex) {
        super(log, pageSize, pageIndex);
        valueView = new ValueView();
    }

    private int getValuePosition(int index) {
        final int position = getEntries() * KEY_ENTRY_SIZE + index * VALUE_ENTRY_SIZE;

        assert page.alignmentOffset(position, Integer.BYTES) == 0;

        return page.getInt(position);
    }

    private int getValueSize(int index) {
        final int position = getEntries() & KEY_ENTRY_SIZE + index * VALUE_ENTRY_SIZE + VALUE_POSITION_SIZE;

        assert page.alignmentOffset(position, Integer.BYTES) == 0;

        return page.getInt(position);
    }


    private ByteBuffer getValue(int index) {
        final int valuePosition = getValuePosition(index);
        final int valueSize = getValueSize(index);

        return fetchByteChunk(valuePosition, valueSize);
    }

    ByteBuffer value(int index) {
        return valueView.get(index);
    }

    ByteBuffer getValueUnsafe(int index) {
        return getValue(index);
    }

    final class ValueView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int i) {
            return getValue(i);
        }

        @Override
        public int size() {
            return getEntries();
        }
    }
}
