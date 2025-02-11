/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.TreeCursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

/**
 * Base BTree implementation
 */
public abstract class BTreeBase implements ITree {

    public static final byte BOTTOM_ROOT = 2;
    public static final byte INTERNAL_ROOT = 3;
    public static final byte BOTTOM = 4;
    public static final byte INTERNAL = 5;
    public static final byte LEAF = 6;

    public static final byte LEAF_DUP_BOTTOM_ROOT = 7;
    public static final byte LEAF_DUP_INTERNAL_ROOT = 8;
    public static final byte DUP_BOTTOM = 9;
    public static final byte DUP_INTERNAL = 10;
    public static final byte DUP_LEAF = 11;

    @NotNull
    protected final Log log;
    protected DataIterator dataIterator = null;
    @NotNull
    protected final BTreeBalancePolicy balancePolicy;
    protected final boolean allowsDuplicates;
    protected long size = -1;
    protected final int structureId;
    protected final int maxEntrySize;

    BTreeBase(@NotNull final Log log, @NotNull final BTreeBalancePolicy balancePolicy, final boolean allowsDuplicates,
              final int structureId, final int maxEntrySize) {
        this.log = log;
        this.balancePolicy = balancePolicy;
        this.allowsDuplicates = allowsDuplicates;
        this.structureId = structureId;
        this.maxEntrySize = maxEntrySize;
    }

    @Override
    @NotNull
    public abstract BTreeMutable getMutableCopy();

    /**
     * Returns root page of the tree
     *
     * @return tree root
     */
    @NotNull
    protected abstract BasePage getRoot();

    @Override
    public int getStructureId() {
        return structureId;
    }

    @Override
    public boolean isEmpty() {
        return getRoot().getSize() == 0;
    }

    @Override
    @NotNull
    public Log getLog() {
        return log;
    }

    @NotNull
    @Override
    public DataIterator getDataIterator(long address) {
        if (dataIterator == null) {
            dataIterator = new DataIterator(log, address);
        } else {
            if (address >= 0L) {
                dataIterator.checkPage(address);
            }
        }
        return dataIterator;
    }

    @NotNull
    public BTreeBalancePolicy getBalancePolicy() {
        return balancePolicy;
    }

    @Override
    public AddressIterator addressIterator() {
        final BTreeTraverser traverser = allowsDuplicates ? BTreeMutatingTraverserDup.create(this) : BTreeMutatingTraverser.create(this);
        return new AddressIterator(isEmpty() ? null : this, traverser.currentPos >= 0 && !isEmpty(), traverser);
    }

    @Override
    public ITreeCursor openCursor() {
        return allowsDuplicates ?
                new BTreeCursorDup(new BTreeTraverserDup(getRoot())) :
                new TreeCursor(new BTreeTraverser(getRoot()));
    }

    protected final RandomAccessLoggable getLoggable(long address) {
        return log.readNotNull(getDataIterator(address), address);
    }

    @NotNull
    protected final BasePageImmutable loadPage(final long address) {
        final RandomAccessLoggable loggable = getLoggable(address);
        return loadPage(loggable.getType(), loggable.getData(), loggable.isDataInsideSinglePage());
    }

    @NotNull
    protected final BasePageImmutable loadPage(final int type, @NotNull final ByteIterableWithAddress data,
                                               final boolean loggableInsideSinglePage) {
        final BasePageImmutable result;
        switch (type) {
            case LEAF_DUP_BOTTOM_ROOT: // TODO: convert to enum
            case BOTTOM_ROOT:
            case BOTTOM:
            case DUP_BOTTOM:
                result = new BottomPage(this, data, loggableInsideSinglePage);
                break;
            case LEAF_DUP_INTERNAL_ROOT:
            case INTERNAL_ROOT:
            case INTERNAL:
            case DUP_INTERNAL:
                result = new InternalPage(this, data, loggableInsideSinglePage);
                break;
            default:
                throw new IllegalArgumentException("Unknown loggable type [" + type + ']');
        }
        return result;
    }

    @NotNull
    protected LeafNode loadLeaf(final long address) {
        final RandomAccessLoggable loggable = getLoggable(address);
        final byte type = loggable.getType();
        switch (type) {
            case LEAF:
            case DUP_LEAF:
                return new LeafNode(log, loggable);
            case LEAF_DUP_BOTTOM_ROOT:
            case LEAF_DUP_INTERNAL_ROOT:
                if (allowsDuplicates) {
                    return new LeafNodeDup(this, loggable);
                } else {
                    throw new ExodusException("Try to load leaf with duplicates, but tree is not configured " +
                            "to support duplicates.");
                }
            default:
                DataCorruptionException.raise("Unexpected loggable type: " + type, log, address);
                // dummy unreachable statement
                throw new RuntimeException();
        }
    }

    protected boolean isDupKey(final long address) {
        final byte type = getLoggable(address).getType();
        return type == LEAF_DUP_BOTTOM_ROOT || type == LEAF_DUP_INTERNAL_ROOT;
    }

    int compareLeafToKey(final long address, @NotNull final ByteIterable key) {
        final RandomAccessLoggable loggable = getLoggable(address);
        final ByteIterableWithAddress data = loggable.getData();
        final int keyLength = data.getCompressedUnsignedInt();
        final int keyRecordSize = CompressedUnsignedLongByteIterable.getCompressedSize(keyLength);
        return data.compareTo(keyRecordSize, keyLength, key, 0, key.getLength());
    }

    @Override
    @Nullable
    public ByteIterable get(final @NotNull ByteIterable key) {
        final ILeafNode leaf = getRoot().get(key);
        return leaf == null ? null : leaf.getValue();
    }

    @Override
    public boolean hasKey(@NotNull final ByteIterable key) {
        return getRoot().keyExists(key);
    }

    @Override
    public boolean hasPair(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return getRoot().exists(key, value);
    }

    @Override
    public void dump(PrintStream out) {
        getRoot().dump(out, 0, null);
    }

    @Override
    public void dump(PrintStream out, INode.ToString renderer) {
        getRoot().dump(out, 0, renderer);
    }

    @Override
    public long getSize() {
        return size;
    }
}
