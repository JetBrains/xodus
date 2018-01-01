/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.*;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class BTreeMutable extends BTreeBase implements ITreeMutable {

    private static final int MAX_EXPIRED_LOGGABLES_TO_CONTINUE_RECLAIM_ON_A_NEW_FILE = 100000;

    @NotNull
    private BasePageMutable root;
    private Collection<ExpiredLoggableInfo> expiredLoggables = null;
    @Nullable
    private List<ITreeCursorMutable> openCursors = null;
    @NotNull
    private final BTreeBase immutableTree;
    private final LightOutputStream leafStream;

    BTreeMutable(@NotNull final BTreeBase tree) {
        super(tree.log, tree.balancePolicy, tree.allowsDuplicates, tree.structureId);
        immutableTree = tree;
        leafStream = new LightOutputStream(16);
        root = tree.getRoot().getMutableCopy(this);
        size = tree.getSize();
    }

    @Override
    public long getRootAddress() {
        return Loggable.NULL_ADDRESS;
    }

    @Override
    @NotNull
    public BasePageMutable getRoot() {
        return root;
    }

    @Override
    public boolean isAllowingDuplicates() {
        return allowsDuplicates;
    }

    @SuppressWarnings({"ReturnOfCollectionOrArrayField"})
    @Override
    @Nullable
    public Iterable<ITreeCursorMutable> getOpenCursors() {
        return openCursors;
    }

    @Override
    public AddressIterator addressIterator() {
        return immutableTree.addressIterator();
    }

    @Override
    @NotNull
    public BTreeMutable getMutableCopy() {
        return this;
    }

    @Override
    public void put(@NotNull INode ln) {
        final ByteIterable value = ln.getValue();
        if (value == null) {
            throw new ExodusException("Value can't be null");
        }
        put(ln.getKey(), value);
    }

    @Override
    public void putRight(@NotNull INode ln) {
        final ByteIterable value = ln.getValue();
        if (value == null) {
            throw new ExodusException("Value can't be null");
        }
        putRight(ln.getKey(), value);
    }

    @Override
    public boolean add(@NotNull INode ln) {
        final ByteIterable value = ln.getValue();
        if (value == null) {
            throw new ExodusException("Value can't be null");
        }
        return add(ln.getKey(), value);
    }

    @Override
    public boolean put(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return put(key, value, true);
    }

    @Override
    public void putRight(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        BasePageMutable newSibling = root.putRight(key, value);
        if (newSibling != null) {
            root = new InternalPageMutable(this, root, newSibling);
        }
    }

    @Override
    public boolean add(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return put(key, value, false);
    }

    private boolean put(@NotNull final ByteIterable key, @NotNull final ByteIterable value, boolean overwrite) {
        boolean[] result = {false};
        BasePageMutable newSibling = root.put(key, value, overwrite, result);
        if (newSibling != null) {
            root = new InternalPageMutable(this, root, newSibling);
        }

        return result[0];
    }

    @Override
    public boolean delete(@NotNull ByteIterable key) {
        return deleteImpl(key, null);
    }

    @Override
    public boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value, @Nullable ITreeCursorMutable cursorToSkip) {
        if (deleteImpl(key, value)) {
            TreeCursorMutable.notifyCursors(this, cursorToSkip);
            return true;
        }
        return false;
    }

    LightOutputStream getLeafStream() {
        return leafStream;
    }

    // for test only!!!
    protected boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value) {
        if (deleteImpl(key, value)) {
            TreeCursorMutable.notifyCursors(this);
            return true;
        }
        return false;
    }

    private boolean deleteImpl(ByteIterable key, @Nullable ByteIterable value) {
        boolean[] res = new boolean[1];
        root = delete(root, key, value, res);
        return res[0];
    }

    protected void decrementSize(final long delta) {
        size -= delta;
    }

    protected void incrementSize() {
        size++;
    }

    static BasePageMutable delete(BasePageMutable root, ByteIterable key, @Nullable ByteIterable value, boolean[] res) {
        if (root.delete(key, value)) {
            root = root.mergeWithChildren();
            res[0] = true;
            return root;
        }

        res[0] = false;
        return root;
    }

    @Override
    public long save() {
        // dfs, save leafs, then bottoms, then internals, then root
        final byte type = root.isBottom() ? BOTTOM_ROOT : INTERNAL_ROOT;
        final Log log = getLog();

        final ByteIterable savedData = root.getData();

        final ByteIterable[] iterables = {
            CompressedUnsignedLongByteIterable.getIterable(size),
            savedData
        };

        return log.write(type, structureId, new CompoundByteIterable(iterables));
    }

    protected void addExpiredLoggable(@NotNull Loggable loggable) {
        getExpiredLoggables().add(new ExpiredLoggableInfo(loggable));
    }

    protected void addExpiredLoggable(long address) {
        if (address != Loggable.NULL_ADDRESS) addExpiredLoggable(getLoggable(address));
    }

    @SuppressWarnings({"ReturnOfCollectionOrArrayField"})
    @Override
    @NotNull
    public Collection<ExpiredLoggableInfo> getExpiredLoggables() {
        if (expiredLoggables == null) {
            expiredLoggables = new ArrayList<>(16);
        }
        return expiredLoggables;
    }

    @Override
    public TreeCursor openCursor() {
        final List<ITreeCursorMutable> cursors;
        if (openCursors == null) {
            cursors = new ArrayList<>(4);
            openCursors = cursors;
        } else {
            cursors = openCursors;
        }
        final TreeCursorMutable result = allowsDuplicates ?
            new BTreeCursorDupMutable(this, new BTreeTraverserDup(root)) :
            new TreeCursorMutable(this, new BTreeTraverser(root));
        cursors.add(result);
        return result;
    }

    @SuppressWarnings({"ConstantConditions"})
    @Override
    public void cursorClosed(@NotNull final ITreeCursorMutable cursor) {
        openCursors.remove(cursor);
    }

    protected byte getBottomPageType() {
        return BOTTOM;
    }

    protected byte getInternalPageType() {
        return INTERNAL;
    }

    protected byte getLeafType() {
        return LEAF;
    }

    @NotNull
    protected BaseLeafNodeMutable createMutableLeaf(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return new LeafNodeMutable(key, value);
    }

    @Override
    public boolean reclaim(@NotNull RandomAccessLoggable loggable,
                           @NotNull final Iterator<RandomAccessLoggable> loggables) {
        final BTreeReclaimTraverser context = new BTreeReclaimTraverser(this);

        loop:
        while (true) {
            final byte type = loggable.getType();
            switch (type) {
                case NullLoggable.TYPE:
                    break;
                case LEAF_DUP_BOTTOM_ROOT:
                case LEAF_DUP_INTERNAL_ROOT:
                    context.dupLeafsLo.clear();
                    context.dupLeafsHi.clear();
                    new LeafNodeDup(this, loggable).reclaim(context);
                    break;
                case LEAF:
                    new LeafNode(loggable).reclaim(context);
                    break;
                case BOTTOM_ROOT:
                case INTERNAL_ROOT:
                    if (loggable.getAddress() == immutableTree.getRootAddress()) {
                        context.wasReclaim = true;
                    }
                    break loop; // txn ended
                case BOTTOM:
                    reclaimBottom(loggable, context);
                    break;
                case INTERNAL:
                    reclaimInternal(loggable, context);
                    break;
                case DUP_LEAF:
                case DUP_BOTTOM:
                case DUP_INTERNAL:
                    context.dupLeafsLo.clear();
                    context.dupLeafsHi.clear();
                    final RandomAccessLoggable leaf = LeafNodeDup.collect(context.dupLeafsHi, loggable, loggables);
                    if (leaf == null) {
                        break loop; // loggable of dup leaf type not found, txn ended prematurely
                    }
                    new LeafNodeDup(this, leaf).reclaim(context);
                    break;
                default:
                    throw new ExodusException("Unexpected loggable type " + type);
            }
            if (!loggables.hasNext()) {
                break;
            }
            // if we have reached the end of file and the tree seems to be rather heavyweight then looks like
            // it was a huge transaction that saved the tree, and it's reasonable to stop here, without
            // reaching the tree's root, in order to avoid possible OOME (XD-513)
            if (type == NullLoggable.TYPE &&
                expiredLoggables != null && // this check fixes XD-532 & XD-538
                expiredLoggables.size() > MAX_EXPIRED_LOGGABLES_TO_CONTINUE_RECLAIM_ON_A_NEW_FILE) {
                break;
            }
            loggable = loggables.next();
        }

        while (context.canMoveUp()) {
            // wire up mutated stuff
            context.popAndMutate();
        }

        return context.wasReclaim;
    }

    void reclaimInternal(RandomAccessLoggable loggable, BTreeReclaimTraverser context) {
        final ByteIterableWithAddress data = loggable.getData();
        final ByteIteratorWithAddress it = data.iterator();
        final int i = CompressedUnsignedLongByteIterable.getInt(it);
        if ((i & 1) == 1 && i > 1) {
            final LeafNode minKey = loadMinKey(data, CompressedUnsignedLongByteIterable.getCompressedSize(i));
            if (minKey != null) {
                final InternalPage page = new InternalPage(this, data.clone((int) (it.getAddress() - data.getDataAddress())), i >> 1);
                page.reclaim(minKey.getKey(), context);
            }
        }
    }

    void reclaimBottom(RandomAccessLoggable loggable, BTreeReclaimTraverser context) {
        final ByteIterableWithAddress data = loggable.getData();
        final ByteIteratorWithAddress it = data.iterator();
        final int i = CompressedUnsignedLongByteIterable.getInt(it);
        if ((i & 1) == 1 && i > 1) {
            final LeafNode minKey = loadMinKey(data, CompressedUnsignedLongByteIterable.getCompressedSize(i));
            if (minKey != null) {
                final BottomPage page = new BottomPage(this, data.clone((int) (it.getAddress() - data.getDataAddress())), i >> 1);
                page.reclaim(minKey.getKey(), context);
            }
        }
    }

    @Nullable
    private LeafNode loadMinKey(ByteIterableWithAddress data, int offset) {
        final int addressLen = data.byteAt(offset);
        final long keyAddress = data.nextLong(offset + 1, addressLen);
        return log.hasAddress(keyAddress) ? loadLeaf(keyAddress) : null;
    }
}
