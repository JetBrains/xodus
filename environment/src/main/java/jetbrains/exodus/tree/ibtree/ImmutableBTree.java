package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteBufferIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class ImmutableBTree implements ITree {
    static final int LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET = Long.BYTES;

    public static final byte KEY_NODE = 44;
    public static final byte VALUE_NODE = 45;

    public static final byte INTERNAL_PAGE = 46;
    public static final byte LEAF_PAGE = 47;

    public static final byte INTERNAL_ROOT_PAGE = 48;
    public static final byte LEAF_ROOT_PAGE = 49;

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

        if (rootAddress == Loggable.NULL_ADDRESS) {
            this.root = null;
        } else {
            this.root = loadPage(rootAddress);
        }
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
        if (root == null) {
            return Loggable.NULL_ADDRESS;
        }

        return root.address;
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
        if (root == null) {
            return null;
        }

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
        var loggable = log.readLoggableAsPage(pageAddress);
        var page = loggable.getBuffer();

        assert page.order() == ByteOrder.nativeOrder();

        var type = loggable.getType();
        if (type == INTERNAL_PAGE || type == INTERNAL_ROOT_PAGE) {
            return new ImmutableInternalPage(log, page, pageAddress);
        } else if (type == LEAF_PAGE || type == LEAF_ROOT_PAGE) {
            return new ImmutableLeafPage(log, page, pageAddress);
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
        return new MutableBTree(this);
    }

    @Override
    public boolean isEmpty() {
        return root.getTreeSize() == 0;
    }

    @Override
    public long getSize() {
        if (root == null) {
            return 0;
        }

        return root.getTreeSize();
    }

    @Override
    public ITreeCursor openCursor() {
        if (root == null) {
            return ITreeCursor.EMPTY_CURSOR;
        }

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
            if (root == null) {
                return;
            }

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

            var address = page.address;
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