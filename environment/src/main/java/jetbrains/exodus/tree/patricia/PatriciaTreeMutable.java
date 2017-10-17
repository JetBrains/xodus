/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

final class PatriciaTreeMutable extends PatriciaTreeBase implements ITreeMutable {

    private MutableRoot root;
    @Nullable
    private Collection<ExpiredLoggableInfo> expiredLoggables;
    private List<ITreeCursorMutable> openCursors = null;

    PatriciaTreeMutable(@NotNull final Log log,
                        final int structureId,
                        final long treeSize,
                        @NotNull final ImmutableNode immutableRoot) {
        super(log, structureId);
        size = treeSize;
        root = new MutableRoot(immutableRoot);
        expiredLoggables = null;
        addExpiredLoggable(immutableRoot.getAddress()); //TODO: don't re-read
    }

    @Override
    public long getRootAddress() {
        return Loggable.NULL_ADDRESS;
    }

    @NotNull
    @Override
    public PatriciaTreeMutable getMutableCopy() {
        return this;
    }

    @Override
    public boolean put(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        final ByteIterator it = key.iterator();
        MutableNode node = root;
        MutableNode prev = null;
        byte prevFirstByte = (byte) 0;
        boolean result = false;
        while (true) {
            final long matchResult = node.matchesKeySequence(it);
            final int matchingLength = NodeBase.MatchResult.getMatchingLength(matchResult);
            if (matchingLength < 0) {
                final MutableNode prefix = node.splitKey(-matchingLength - 1, NodeBase.MatchResult.getKeyByte(matchResult));
                if (NodeBase.MatchResult.hasNext(matchResult)) {
                    prefix.hang(NodeBase.MatchResult.getNextByte(matchResult), it).setValue(value);
                } else {
                    prefix.setValue(value);
                }
                if (prev == null) {
                    root = new MutableRoot(prefix, root.sourceAddress);
                } else {
                    prev.setChild(prevFirstByte, prefix);
                }
                ++size;
                result = true;
                break;
            }
            if (!it.hasNext()) {
                final ByteIterable oldValue = node.getValue();
                node.setValue(value);
                if (oldValue == null) {
                    ++size;
                    result = true;
                }
                break;
            }
            final byte nextByte = it.next();
            final NodeBase child = node.getChild(this, nextByte);
            if (child == null) {
                if (node.hasChildren() || node.hasKey() || node.hasValue()) {
                    node.hang(nextByte, it).setValue(value);
                } else {
                    node.setKeySequence(new ArrayByteIterable(nextByte, it));
                    node.setValue(value);
                }
                ++size;
                result = true;
                break;
            }
            prev = node;
            prevFirstByte = nextByte;
            final MutableNode mutableChild = child.getMutableCopy(this);
            if (!child.isMutable()) {
                node.setChild(nextByte, mutableChild);
            }
            node = mutableChild;
        }
        return result;
    }

    @Override
    public void putRight(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        final ByteIterator it = key.iterator();
        MutableNode node = root;
        MutableNode prev = null;
        byte prevFirstByte = (byte) 0;
        while (true) {
            final long matchResult = node.matchesKeySequence(it);
            final int matchingLength = NodeBase.MatchResult.getMatchingLength(matchResult);
            if (matchingLength < 0) {
                if (!NodeBase.MatchResult.hasNext(matchResult)) {
                    throw new IllegalArgumentException();
                }
                final MutableNode prefix = node.splitKey(-matchingLength - 1, NodeBase.MatchResult.getKeyByte(matchResult));
                prefix.hangRight(NodeBase.MatchResult.getNextByte(matchResult), it).setValue(value);
                if (prev == null) {
                    root = new MutableRoot(prefix, root.sourceAddress);
                } else {
                    prev.setChild(prevFirstByte, prefix);
                }
                ++size;
                break;
            }
            if (!it.hasNext()) {
                if (node.hasChildren() || node.hasValue()) {
                    throw new IllegalArgumentException();
                }
                node.setValue(value);
                ++size;
                break;
            }
            final byte nextByte = it.next();
            final NodeBase child = node.getRightChild(this, nextByte);
            if (child == null) {
                if (node.hasChildren() || node.hasKey() || node.hasValue()) {
                    node.hangRight(nextByte, it).setValue(value);
                } else {
                    node.setKeySequence(new ArrayByteIterable(nextByte, it));
                    node.setValue(value);
                }
                ++size;
                break;
            }
            prev = node;
            prevFirstByte = nextByte;
            final MutableNode mutableChild = child.getMutableCopy(this);
            if (!child.isMutable()) {
                node.setRightChild(nextByte, mutableChild);
            }
            node = mutableChild;
        }
    }

    @Override
    public boolean add(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        final ByteIterator it = key.iterator();
        NodeBase node = root;
        MutableNode mutableNode = null;
        final Deque<ChildReferenceTransient> stack = new ArrayDeque<>();
        while (true) {
            final long matchResult = node.matchesKeySequence(it);
            final int matchingLength = NodeBase.MatchResult.getMatchingLength(matchResult);
            if (matchingLength < 0) {
                final MutableNode prefix = node.getMutableCopy(this).
                    splitKey(-matchingLength - 1, NodeBase.MatchResult.getKeyByte(matchResult));
                if (NodeBase.MatchResult.hasNext(matchResult)) {
                    prefix.hang(NodeBase.MatchResult.getNextByte(matchResult), it).setValue(value);
                } else {
                    prefix.setValue(value);
                }
                if (stack.isEmpty()) {
                    root = new MutableRoot(prefix, root.sourceAddress);
                } else {
                    final ChildReferenceTransient parent = stack.pop();
                    mutableNode = parent.mutate(this);
                    mutableNode.setChild(parent.firstByte, prefix);
                }
                break;
            }
            if (!it.hasNext()) {
                if (node.hasValue()) {
                    return false;
                }
                mutableNode = node.getMutableCopy(this);
                mutableNode.setValue(value);
                break;
            }
            final byte nextByte = it.next();
            final NodeBase child = node.getChild(this, nextByte);
            if (child == null) {
                mutableNode = node.getMutableCopy(this);
                if (mutableNode.hasChildren() || mutableNode.hasKey() || mutableNode.hasValue()) {
                    mutableNode.hang(nextByte, it).setValue(value);
                } else {
                    mutableNode.setKeySequence(new ArrayByteIterable(nextByte, it));
                    mutableNode.setValue(value);
                }
                break;
            }
            stack.push(new ChildReferenceTransient(nextByte, node));
            node = child;
        }
        ++size;
        mutateUp(stack, mutableNode);
        return true;
    }

    @Override
    public boolean delete(@NotNull final ByteIterable key) {
        return deleteImpl(key);
    }

    @Override
    public boolean delete(@NotNull final ByteIterable key,
                          @Nullable final ByteIterable value,
                          @Nullable final ITreeCursorMutable cursorToSkip) {
        if (value == null) {
            if (deleteImpl(key)) {
                TreeCursorMutable.notifyCursors(this, cursorToSkip);
                return true;
            }
            return false;
        }
        throw new UnsupportedOperationException("Patricia tree doesn't support duplicates!");
    }

    @Override
    public void put(@NotNull final INode ln) {
        put(ln.getKey(), getNotNullValue(ln));
    }

    @Override
    public void putRight(@NotNull final INode ln) {
        putRight(ln.getKey(), getNotNullValue(ln));
    }

    @Override
    public boolean add(@NotNull final INode ln) {
        return add(ln.getKey(), getNotNullValue(ln));
    }

    @Override
    public long save() {
        return root.save(this, new MutableNodeSaveContext(CompressedUnsignedLongByteIterable.getIterable(size)));
    }

    @SuppressWarnings({"NullableProblems"})
    @NotNull
    @Override
    public Collection<ExpiredLoggableInfo> getExpiredLoggables() {
        final Collection<ExpiredLoggableInfo> expiredLoggables = this.expiredLoggables;
        return expiredLoggables == null ? Collections.<ExpiredLoggableInfo>emptyList() : expiredLoggables;
    }

    @Override
    public ITreeCursor openCursor() {
        if (openCursors == null) {
            openCursors = new ArrayList<>(4);
        }
        final ITreeCursorMutable result = new TreeCursorMutable(this, new PatriciaTraverser(this, root), root.hasValue());
        openCursors.add(result);
        return result;
    }

    @Override
    public void cursorClosed(@NotNull ITreeCursorMutable cursor) {
        openCursors.remove(cursor);
    }

    @Override
    public boolean reclaim(@NotNull RandomAccessLoggable loggable,
                           @NotNull final Iterator<RandomAccessLoggable> loggables) {
        long minAddress = loggable.getAddress();
        while (true) {
            final byte type = loggable.getType();
            if (type < NODE_WO_KEY_WO_VALUE_WO_CHILDREN || type > MAX_VALID_LOGGABLE_TYPE) {
                if (type != NullLoggable.TYPE) { // skip null loggable
                    throw new ExodusException("Unexpected loggable type " + loggable.getType());
                }
            } else {
                if (loggable.getStructureId() != structureId) {
                    throw new ExodusException("Unexpected structure id " + loggable.getStructureId());
                }
                if (PatriciaTreeBase.nodeIsRoot(type)) {
                    break;
                }
            }
            if (!loggables.hasNext()) {
                return false;
            }
            loggable = loggables.next();
        }

        final long maxAddress = loggable.getAddress();
        final PatriciaTreeForReclaim sourceTree = new PatriciaTreeForReclaim(log, maxAddress, structureId);
        final ImmutableNode sourceRoot = sourceTree.getRoot();

        final long backRef = sourceTree.getBackRef();
        if (backRef > 0) {
            final long treeStartAddress = sourceTree.getRootAddress() - backRef;
            if (treeStartAddress > minAddress) {
                throw new IllegalStateException("Wrong back reference!");
            }
            if (!log.hasAddressRange(treeStartAddress, maxAddress)) {
                return false;
            }
            minAddress = treeStartAddress;
        }

        final PatriciaReclaimActualTraverser actual = new PatriciaReclaimActualTraverser(this);
        reclaim(new PatriciaReclaimSourceTraverser(sourceTree, sourceRoot, minAddress), actual);
        return actual.wasReclaim || sourceRoot.getAddress() == root.sourceAddress;
    }

    @Override
    public MutableRoot getRoot() {
        return root;
    }

    @Override
    public boolean isAllowingDuplicates() {
        return false;
    }

    @SuppressWarnings({"ReturnOfCollectionOrArrayField"})
    @Override
    @Nullable
    public Iterable<ITreeCursorMutable> getOpenCursors() {
        return openCursors;
    }

    MutableNode mutateNode(@NotNull final ImmutableNode node) {
        addExpiredLoggable(node.getAddress()); //TODO: don't re-read
        return new MutableNode(node);
    }

    static ByteIterable getNotNullValue(@NotNull final INode ln) {
        final ByteIterable value = ln.getValue();
        if (value == null) {
            throw new ExodusException("Value can't be null");
        }
        return value;
    }

    private void addExpiredLoggable(long address) {
        if (address != Loggable.NULL_ADDRESS) addExpiredLoggable(getLoggable(address));
    }

    private void addExpiredLoggable(@Nullable final RandomAccessLoggable sourceLoggable) {
        if (sourceLoggable != null) {
            Collection<ExpiredLoggableInfo> expiredLoggables = this.expiredLoggables;
            if (expiredLoggables == null) {
                expiredLoggables = new ArrayList<>(16);
                this.expiredLoggables = expiredLoggables;
            }
            expiredLoggables.add(new ExpiredLoggableInfo(sourceLoggable));
        }
    }

    private boolean deleteImpl(@NotNull final ByteIterable key) {
        final ByteIterator it = key.iterator();
        NodeBase node = root;
        final Deque<ChildReferenceTransient> stack = new ArrayDeque<>();
        for (; ; ) {
            if (node == null || NodeBase.MatchResult.getMatchingLength(node.matchesKeySequence(it)) < 0) {
                return false;
            }
            if (!it.hasNext()) {
                break;
            }
            final byte nextByte = it.next();
            stack.push(new ChildReferenceTransient(nextByte, node));
            node = node.getChild(this, nextByte);
        }
        if (!node.hasValue()) {
            return false;
        }
        --size;
        MutableNode mutableNode = node.getMutableCopy(this);
        ChildReferenceTransient parent = stack.peek();
        final boolean hasChildren = mutableNode.hasChildren();
        if (!hasChildren && parent != null) {
            stack.pop();
            mutableNode = parent.mutate(this);
            mutableNode.removeChild(parent.firstByte);
            if (!mutableNode.hasValue() && mutableNode.getChildrenCount() == 1) {
                mutableNode.mergeWithSingleChild(this);
            }
        } else {
            mutableNode.setValue(null);
            if (!hasChildren) {
                mutableNode.setKeySequence(ByteIterable.EMPTY);
            } else if (mutableNode.getChildrenCount() == 1) {
                mutableNode.mergeWithSingleChild(this);
            }
        }
        mutateUp(stack, mutableNode);
        return true;
    }

    /*
     * stack contains all ancestors of the node, stack.peek() is its parent.
     */
    private void mutateUp(@NotNull final Deque<ChildReferenceTransient> stack, MutableNode node) {
        while (!stack.isEmpty()) {
            final ChildReferenceTransient parent = stack.pop();
            final MutableNode mutableParent = parent.mutate(this);
            mutableParent.setChild(parent.firstByte, node);
            node = mutableParent;
        }
    }

    private static void reclaim(@NotNull final PatriciaReclaimSourceTraverser source,
                                @NotNull final PatriciaReclaimActualTraverser actual) {
        final NodeBase actualNode = actual.currentNode;
        final NodeBase sourceNode = source.currentNode;
        if (actualNode.getAddress() == sourceNode.getAddress()) {
            actual.currentNode = actualNode.getMutableCopy(actual.mainTree);
            actual.getItr();
            actual.wasReclaim = true;
            reclaimActualChildren(source, actual);
        } else {
            @NotNull
            ByteIterator srcItr = sourceNode.keySequence.iterator();
            @NotNull
            ByteIterator actItr = actualNode.keySequence.iterator();
            int srcPushes = 0;
            int actPushes = 0;
            while (true) {
                if (srcItr.hasNext()) {
                    if (actItr.hasNext()) {
                        if (srcItr.next() != actItr.next()) { // key is not matching
                            break;
                        }
                    } else {
                        final NodeChildrenIterator children = actual.currentNode.getChildren(srcItr.next());
                        final ChildReference child = children.getNode();
                        if (child == null) {
                            break;
                        }
                        actual.currentChild = child;
                        actual.currentIterator = children;
                        actual.moveDown();
                        ++actPushes;
                        actItr = actual.currentNode.keySequence.iterator();
                    }
                } else if (actItr.hasNext()) {
                    final NodeChildrenIterator children = source.currentNode.getChildren(actItr.next());
                    final ChildReference child = children.getNode();
                    if (child == null || !source.isAddressReclaimable(child.suffixAddress)) {
                        break; // child can be expired if source parent was already not-current
                    }
                    source.currentChild = child;
                    source.currentIterator = children;
                    source.moveDown();
                    ++srcPushes;
                    srcItr = source.currentNode.keySequence.iterator();
                } else { // both iterators matched, here comes the branching
                    reclaimChildren(source, actual);
                    break;
                }
            }
            for (int i = 0; i < srcPushes; ++i) {
                source.moveUp();
            }
            for (int i = 0; i < actPushes; ++i) {
                actual.popAndMutate();
            }
        }
    }

    private static void reclaimActualChildren(@NotNull final PatriciaReclaimSourceTraverser source,
                                              @NotNull final PatriciaReclaimActualTraverser actual) {
        while (actual.isValidPos()) {
            final ChildReference actualChild = actual.currentChild;
            final long suffixAddress = actualChild.suffixAddress;
            if (source.isAddressReclaimable(suffixAddress)) {
                actual.moveDown();
                actual.currentNode = actual.currentNode.getMutableCopy(actual.mainTree);
                actual.getItr();
                actual.wasReclaim = true;
                reclaimActualChildren(source, actual);
                actual.popAndMutate();
            }
            actual.moveRight();
        }
    }

    private static void reclaimChildren(@NotNull final PatriciaReclaimSourceTraverser source,
                                        @NotNull final PatriciaReclaimActualTraverser actual) {
        source.moveToNextReclaimable();
        while (source.isValidPos() && actual.isValidPos()) {
            final ChildReference sourceChild = source.currentChild;
            final int sourceByte = sourceChild.firstByte & 0xff;
            final int actualByte = actual.currentChild.firstByte & 0xff;
            if (sourceByte < actualByte) {
                source.moveRight();
            } else if (sourceByte > actualByte) {
                actual.moveRight();
            } else {
                source.moveDown();
                actual.moveDown();
                reclaim(source, actual);
                actual.popAndMutate();
                source.moveUp();
                source.moveRight();
                actual.moveRight();
            }
        }
    }
}
