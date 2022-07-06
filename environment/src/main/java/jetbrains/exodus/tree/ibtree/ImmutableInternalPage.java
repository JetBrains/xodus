package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;

/**
 * Presentation of immutable internal page in BTree.
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
 *     <li>Array each entry of which contains index of the child page</li>
 *     <li>Array of keys</li>
 * </ol>
 * <p>
 * <p>
 * Each page is made to be big enough to store at least two key-child index entries.
 * In each key-child index entry, index is index of the page which contains keys which are heigher or equal to the
 * key stored into the entry.
 * Initially only one page is loaded but if that is not enough, because key is too big to be stored
 * inside of the single page additional pages will be loaded from the log.
 */
final class ImmutableInternalPage extends ImmutableBasePage {
    private static final int CHILD_INDEX_SIZE = Long.BYTES;

    ImmutableInternalPage(Log log, int pageSize, long pageIndex) {
        super(log, pageSize, pageIndex);
    }

    long getChildIndex(int index) {
        final int position = getChildIndexPosition(index);

        assert page.alignmentOffset(position, Long.BYTES) == 0;

        return page.getLong(position);
    }

    private int getChildIndexPosition(int index) {
        final int initialOffset = getEntries() * KEY_ENTRY_SIZE + KEYS_OFFSET;
        //align memory access to ensure fastest data processing
        final int alignOffset = page.alignmentOffset(initialOffset, CHILD_INDEX_SIZE);
        return initialOffset + alignOffset + index * CHILD_INDEX_SIZE;
    }
}
