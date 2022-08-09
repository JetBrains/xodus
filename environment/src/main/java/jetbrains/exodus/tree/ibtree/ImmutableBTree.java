package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.*;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteOrder;

public final class ImmutableBTree implements BTree {
    static final int LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET = Long.BYTES;

    public static final byte TWO_BYTES_STUB = 44;
    @SuppressWarnings("unused")
    public static final byte THREE_BYTES_STUB = 45;
    @SuppressWarnings("unused")
    public static final byte FOUR_BYTES_STUB = 46;
    @SuppressWarnings("unused")
    public static final byte FIVE_BYTES_STUB = 47;
    @SuppressWarnings("unused")
    public static final byte SIX_BYTES_STUB = 48;
    public static final byte SEVEN_BYTES_STUB = 49;
    public static final byte EIGHTS_BYTES_AND_MORE_STUB = 50;


    public static final byte INTERNAL_PAGE = 51;
    public static final byte LEAF_PAGE = 52;

    public static final byte INTERNAL_ROOT_PAGE = 53;
    public static final byte LEAF_ROOT_PAGE = 54;

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

    ImmutableBasePage loadPage(long pageAddress) {
        var loggable = log.readLoggableAsPage(pageAddress);
        var page = loggable.getBuffer();

        assert page.order() == ByteOrder.nativeOrder();

        var type = loggable.getType();
        if (type == INTERNAL_PAGE || type == INTERNAL_ROOT_PAGE) {
            return new ImmutableInternalPage(this, log, page, pageAddress);
        } else if (type == LEAF_PAGE || type == LEAF_ROOT_PAGE) {
            return new ImmutableLeafPage(log, page, pageAddress);
        } else {
            throw new IllegalStateException(String.format("Invalid loggable type %d.", type));
        }
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

        return new TreeImmutableCursor(this, this.root);
    }

    @Override
    public LongIterator addressIterator() {
        return new TreeAddressIterator();
    }

    @Override
    public TraversablePage getRoot() {
        return root;
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
            var elemRef = stack.dequeueLast();
            var page = elemRef.page;

            var address = page.address();
            if (!stack.isEmpty()) {
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
            }

            return address;
        }

        private void fetchAncestors(ElemRef elemRef) {
            var child = elemRef.page.child(elemRef.index);

            var childRef = new ElemRef(child, 0);
            stack.enqueue(childRef);

            while (!(child instanceof ImmutableLeafPage)) {
                child = child.child(0);
                childRef = new ElemRef(child, 0);

                stack.enqueue(childRef);
            }
        }
    }
}