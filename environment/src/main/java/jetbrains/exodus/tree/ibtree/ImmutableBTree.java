package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteBufferIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.tree.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class ImmutableBTree implements ITree {
    static final int LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET = 2 * Long.BYTES;

    public static final byte INTERNAL_PAGE = 44;
    public static final byte LEAF_PAGE = 45;
    public static final byte VALUE_NODE = 46;
    public static final byte KEY_NODE = 47;

    @NotNull
    final Log log;
    final int structureId;
    final int pageSize;

    final ImmutableBasePage root;

    DataIterator dataIterator = null;

    ImmutableBTree(@NotNull Log log, int structureId, int pageSize, long rootAddress) {
        this.log = log;
        this.structureId = structureId;
        this.pageSize = pageSize;

        this.root = loadPage(rootAddress);
    }

    @Override
    public @NotNull Log getLog() {
        return log;
    }

    @Override
    public @NotNull DataIterator getDataIterator(long address) {
        if (dataIterator == null) {
            dataIterator = new DataIterator(log, address);
        } else {
            if (address >= 0L) {
                dataIterator.checkPage(address);
            }
        }
        return dataIterator;
    }

    @Override
    public long getRootAddress() {
        return root.pageAddress;
    }

    @Override
    public int getStructureId() {
        return structureId;
    }

    @Override
    public @Nullable ByteIterable get(@NotNull ByteIterable key) {
        var pageIndexPair = find(key.getByteBuffer());
        if (pageIndexPair == null) {
            return null;
        }

        return new ByteBufferByteIterable(pageIndexPair.page.getValue(pageIndexPair.keyIndex));
    }

    private PageIndexPair find(ByteBuffer key) {
        var page = root;
        while (true) {
            int index = page.find(key);

            if (page instanceof ImmutableLeafPage) {
                if (index < 0) {
                    return null;
                }

                return new PageIndexPair((ImmutableLeafPage) page, index);
            }

            //there is no exact match of the key
            if (index < 0) {
                //index of the first page which contains all keys which are bigger than current one
                index = -index - 1;

                if (index > 0) {
                    index--;
                } else {
                    //all keys in the tree bigger than provided
                    return null;
                }
            }

            var childAddress = page.getChildAddress(index);
            page = loadPage(childAddress);
        }
    }

    ImmutableBasePage loadPage(long pageAddress) {
        var page = log.readLoggableAsPage(pageAddress);
        var childPage = page.slice(LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET,
                pageSize - LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET);

        assert page.order() == ByteOrder.nativeOrder();

        var type = page.get(0);
        if (type == INTERNAL_PAGE) {
            return new ImmutableInternalPage(log, childPage, pageAddress);
        } else if (type == LEAF_PAGE) {
            return new ImmutableLeafPage(log, childPage, pageAddress);
        } else {
            throw new IllegalStateException(String.format("Invalid loggable type %d.", type));
        }
    }

    @Override
    public boolean hasPair(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var pageIndexPair = find(key.getByteBuffer());
        if (pageIndexPair == null) {
            return false;
        }

        var pageValue = pageIndexPair.page.getValue(pageIndexPair.keyIndex);
        if (value instanceof ByteBufferIterable) {
            return value.getByteBuffer().compareTo(pageValue) == 0;
        }

        return new ByteBufferByteIterable(pageValue).compareTo(value) == 0;
    }

    @Override
    public boolean hasKey(@NotNull ByteIterable key) {
        return find(key.getByteBuffer()) != null;
    }

    @Override
    public @NotNull ITreeMutable getMutableCopy() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return root.getTreeSize() != 0;
    }

    @Override
    public long getSize() {
        return root.getTreeSize();
    }

    @Override
    public ITreeCursor openCursor() {
        return new TreeImmutableCursor(this);
    }

    @Override
    public LongIterator addressIterator() {
        return new TreeAddressIterator();
    }

    @SuppressWarnings("ClassCanBeRecord")
    private static final class PageIndexPair {
        private final ImmutableLeafPage page;
        private final int keyIndex;

        private PageIndexPair(ImmutableLeafPage page, int keyIndex) {
            this.page = page;
            this.keyIndex = keyIndex;
        }
    }

    private final class TreeAddressIterator implements LongIterator {
        private final ObjectArrayFIFOQueue<ElemRef> stack = new ObjectArrayFIFOQueue<>();


        public TreeAddressIterator() {
            if (root.getEntriesCount() > 0) {
                var rootRef = new ElemRef(root, 0);
                stack.enqueue(rootRef);

                if (!(root instanceof ImmutableLeafPage)) {
                    fetchAncestors(rootRef);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !stack.isEmpty();
        }

        @Override
        public long next() {
            var elemRef = stack.dequeue();
            var page = elemRef.page;

            var address = page.pageAddress;
            var parentRef = stack.last();

            var parentPage = parentRef.page;
            var parentIndex = parentRef.index;

            //if we do not reach end of the parent page
            //iterate over all ancestors of this page
            //otherwise return address of this page in upcoming call
            //to the next.
            parentIndex++;
            if (parentIndex < parentPage.getEntriesCount()) {
                parentRef.index = parentIndex;
                fetchAncestors(parentRef);
            }

            return address;
        }

        private void fetchAncestors(ElemRef elemRef) {
            var childAddress = elemRef.page.getChildAddress(elemRef.index);
            var child = loadPage(childAddress);

            var childRef = new ElemRef(child, 0);
            stack.enqueue(childRef);

            while (!(child instanceof ImmutableLeafPage)) {
                childAddress = child.getChildAddress(0);
                child = loadPage(childAddress);
                childRef = new ElemRef(child, 0);

                stack.enqueue(childRef);
            }
        }
    }

    private static final class ElemRef {
        private final ImmutableBasePage page;
        private int index;

        private ElemRef(ImmutableBasePage page, int index) {
            this.page = page;
            this.index = index;
        }
    }
}