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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.*;
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.log.*;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"ProtectedField"})
class MutableNode extends NodeBase {

    @NotNull
    protected final ChildReferenceSet children;

    MutableNode(@NotNull final ImmutableNode origin) {
        super(origin.keySequence, origin.value);
        children = new ChildReferenceSet();
        copyChildrenFrom(origin);
    }

    MutableNode(@NotNull final ByteIterable keySequence) {
        this(keySequence, null, new ChildReferenceSet());
    }

    protected MutableNode(@NotNull final ByteIterable keySequence,
                          @Nullable final ByteIterable value,
                          @NotNull final ChildReferenceSet children) {
        super(keySequence, value);
        this.children = children;
    }

    void setKeySequence(@NotNull final ByteIterable keySequence) {
        this.keySequence = keySequence;
    }

    void setValue(@Nullable final ByteIterable value) {
        this.value = value;
    }

    @Override
    long getAddress() {
        return Loggable.NULL_ADDRESS;
    }

    @Override
    boolean isMutable() {
        return true;
    }

    @Override
    MutableNode getMutableCopy(@NotNull final PatriciaTreeMutable mutableTree) {
        return this;
    }

    ChildReference getRef(final int pos) {
        return children.referenceAt(pos);
    }

    @Override
    NodeBase getChild(@NotNull final PatriciaTreeBase tree, final byte b) {
        final ChildReference ref = children.get(b);
        return ref == null ? null : ref.getNode(tree);
    }

    @NotNull
    @Override
    NodeChildren getChildren() {
        return new NodeChildren() {
            @Override
            public NodeChildrenIterator iterator() {
                return children.isEmpty() ?
                    new EmptyNodeChildrenIterator() : new MutableNodeChildrenIterator(MutableNode.this, children);
            }
        };
    }

    @Override
    @NotNull
    NodeChildrenIterator getChildren(final byte b) {
        final int index = children.searchFor(b);
        return index < 0 ? new EmptyNodeChildrenIterator() : new MutableNodeChildrenIterator(this, children.iterator(index));
    }

    @NotNull
    @Override
    NodeChildrenIterator getChildrenLast() {
        return getChildren(children.size());
    }

    @NotNull
    NodeChildrenIterator getChildren(final int pos) {
        return new MutableNodeChildrenIterator(this, children.iterator(pos));
    }

    @Override
    @NotNull
    NodeChildrenIterator getChildrenRange(final byte b) {
        if (children.isEmpty()) {
            return new EmptyNodeChildrenIterator();
        }
        int index = children.searchFor(b);
        if (index < 0) {
            index = -index - 1;
        }
        return new MutableNodeChildrenIterator(this, children.iterator(index));
    }

    @Override
    int getChildrenCount() {
        return children.size();
    }

    boolean hasChildren() {
        return !children.isEmpty();
    }

    void setChild(final byte b, @NotNull final MutableNode child) {
        final int index = children.searchFor(b);
        if (index < 0) {
            children.insertAt(-index - 1, new ChildReferenceMutable(b, child));
        } else {
            final ChildReference ref = children.referenceAt(index);
            if (ref.isMutable()) {
                ((ChildReferenceMutable) ref).child = child;
            } else {
                children.setAt(index, new ChildReferenceMutable(b, child));
            }
        }
    }

    void setChild(final int index, @NotNull final MutableNode child) {
        final ChildReference ref = children.referenceAt(index);
        if (ref.isMutable()) {
            ((ChildReferenceMutable) ref).child = child;
        } else {
            children.setAt(index, new ChildReferenceMutable(ref.firstByte, child));
        }
    }

    NodeBase getRightChild(@NotNull final PatriciaTreeBase tree, final byte b) {
        final ChildReference ref = children.getRight();
        if (ref == null) {
            return null;
        }
        final int rightByte = b & 0xff;
        final int firstByte = ref.firstByte & 0xff;
        if (rightByte < firstByte) {
            throw new IllegalArgumentException();
        }
        return rightByte > firstByte ? null : ref.getNode(tree);
    }

    /**
     * Adds child which is greater (i.e. its next byte greater) than any other.
     *
     * @param b     next byte of child suffix.
     * @param child child node.
     */
    void addRightChild(final byte b, @NotNull final MutableNode child) {
        final ChildReference right = children.getRight();
        if (right != null && (right.firstByte & 0xff) >= (b & 0xff)) {
            throw new IllegalArgumentException();
        }
        children.putRight(new ChildReferenceMutable(b, child));
    }

    /**
     * Sets in-place the right child with the same first byte.
     *
     * @param b     next byte of child suffix.
     * @param child child node.
     */
    void setRightChild(final byte b, @NotNull final MutableNode child) {
        final ChildReference right = children.getRight();
        if (right == null || (right.firstByte & 0xff) != (b & 0xff)) {
            throw new IllegalArgumentException();
        }
        children.setAt(children.size() - 1, new ChildReferenceMutable(b, child));
    }

    boolean removeChild(final byte b) {
        return children.remove(b);
    }

    /**
     * Splits current node onto two ones: prefix defined by prefix length and suffix linked with suffix via nextByte.
     *
     * @param prefixLength length of the prefix.
     * @param nextByte     next byte after prefix linking it with suffix.
     * @return the prefix node.
     */
    MutableNode splitKey(final int prefixLength, final byte nextByte) {
        final byte[] keyBytes = keySequence.getBytesUnsafe();
        final ByteIterable prefixKey;
        if (prefixLength == 0) {
            prefixKey = ByteIterable.EMPTY;
        } else if (prefixLength == 1) {
            prefixKey = SingleByteIterable.getIterable(keyBytes[0]);
        } else {
            prefixKey = new ArrayByteIterable(keyBytes, prefixLength);
        }
        final MutableNode prefix = new MutableNode(prefixKey);
        final int suffixLength = keySequence.getLength() - prefixLength - 1;
        final ByteIterable suffixKey;
        if (suffixLength == 0) {
            suffixKey = ByteIterable.EMPTY;
        } else if (suffixLength == 1) {
            suffixKey = SingleByteIterable.getIterable(keyBytes[prefixLength + 1]);
        } else {
            suffixKey = keySequence.subIterable(prefixLength + 1, suffixLength);
        }
        final MutableNode suffix = new MutableNode(suffixKey, value,
            // copy children of this node to the suffix one
            children);
        prefix.setChild(nextByte, suffix);
        return prefix;
    }

    MutableNode splitKey(final int prefixLength, final int nextByte) {
        return splitKey(prefixLength, (byte) nextByte);
    }

    void mergeWithSingleChild(@NotNull final PatriciaTreeMutable tree) {
        final ChildReference ref = getChildren().iterator().next();
        final NodeBase child = ref.getNode(tree);
        value = child.value;
        keySequence = new CompoundByteIterable(new ByteIterable[]{
            keySequence, SingleByteIterable.getIterable(ref.firstByte), child.keySequence});
        copyChildrenFrom(child);
    }

    MutableNode hang(final byte firstByte, @NotNull final ByteIterator tail) {
        final MutableNode result = new MutableNode(new ArrayByteIterable(tail));
        setChild(firstByte, result);
        return result;
    }

    MutableNode hang(final int firstByte, @NotNull final ByteIterator tail) {
        return hang((byte) firstByte, tail);
    }

    MutableNode hangRight(final byte firstByte, @NotNull final ByteIterator tail) {
        final MutableNode result = new MutableNode(new ArrayByteIterable(tail));
        addRightChild(firstByte, result);
        return result;
    }

    MutableNode hangRight(final int firstByte, @NotNull final ByteIterator tail) {
        return hangRight((byte) firstByte, tail);
    }

    @SuppressWarnings({"OverlyLongMethod"})
    long save(@NotNull final PatriciaTreeMutable tree, @NotNull final MutableNodeSaveContext context) {
        final Log log = tree.getLog();
        // save children and compute number of bytes to represent children's addresses
        int bytesPerAddress = 0;
        for (final ChildReference ref : children) {
            if (ref.isMutable()) {
                ref.suffixAddress = ((ChildReferenceMutable) ref).child.save(tree, context);
            }
            final int logarithm = CompressedUnsignedLongArrayByteIterable.logarithm(ref.suffixAddress);
            if (logarithm > bytesPerAddress) {
                bytesPerAddress = logarithm;
            }
        }
        final int childrenCount = getChildrenCount();
        final LightOutputStream nodeStream = context.newNodeStream();
        // save key and value
        if (hasKey()) {
            CompressedUnsignedLongByteIterable.fillBytes(keySequence.getLength(), nodeStream);
            ByteIterableBase.fillBytes(keySequence, nodeStream);
        }
        if (hasValue()) {
            // noinspection ConstantConditions
            CompressedUnsignedLongByteIterable.fillBytes(value.getLength(), nodeStream);
            // noinspection ConstantConditions
            ByteIterableBase.fillBytes(value, nodeStream);
        }
        if (!children.isEmpty()) {
            // save references to children
            CompressedUnsignedLongByteIterable.fillBytes((childrenCount << 3) + bytesPerAddress - 1, nodeStream);
            for (final ChildReference ref : children) {
                nodeStream.write(ref.firstByte);
                LongBinding.writeUnsignedLong(ref.suffixAddress, bytesPerAddress, nodeStream);
            }
        }
        // finally, write loggable
        byte type = getLoggableType();
        final int structureId = tree.getStructureId();
        final ByteIterable mainIterable = nodeStream.asArrayByteIterable();
        final long startAddress = context.startAddress;
        long result;
        if (!isRoot()) {
            result = log.write(type, structureId, mainIterable);
            // save address of the first saved loggable
            if (startAddress == Loggable.NULL_ADDRESS) {
                context.startAddress = result;
            }
            return result;
        }
        final ByteIterable[] iterables = new ByteIterable[3];
        iterables[0] = context.preliminaryRootData;
        // Save root without back reference if the tree consists of root only.
        // In that case, startAddress is undefined, i.e. no other child is saved before root.
        if (startAddress == Loggable.NULL_ADDRESS) {
            iterables[1] = mainIterable;
            result = log.write(type, structureId, new CompoundByteIterable(iterables, 2));
            return result;
        }
        // Tree is saved with several loggables. Is it saved in a single file?
        final boolean singleFile = log.isLastFileAddress(startAddress);
        final int pos; // where the offset info will be inserted
        if (!singleFile) {
            pos = 1;
            iterables[2] = mainIterable;
        } else {
            iterables[1] = mainIterable;
            result = log.tryWrite(type, structureId, new CompoundByteIterable(iterables, 2));
            if (result >= 0) {
                return result;
            }
            pos = 1;
            iterables[2] = mainIterable;
        }
        type += PatriciaTreeBase.ROOT_BIT_WITH_BACKREF;
        iterables[pos] = CompressedUnsignedLongByteIterable.getIterable(log.getHighAddress() - startAddress);
        final ByteIterable data = new CompoundByteIterable(iterables, pos + 2);
        result = singleFile ? log.writeContinuously(type, structureId, data) : log.tryWrite(type, structureId, data);
        if (result < 0) {
            if (!singleFile) {
                iterables[pos] = CompressedUnsignedLongByteIterable.getIterable(log.getHighAddress() - startAddress);
                result = log.writeContinuously(type, structureId, new CompoundByteIterable(iterables, pos + 2));
                if (result >= 0) {
                    return result;
                }
            }
            throw new TooBigLoggableException();
        }
        return result;
    }

    protected boolean isRoot() {
        return false;
    }

    private void copyChildrenFrom(@NotNull final NodeBase node) {
        final int childrenCount = node.getChildrenCount();
        children.clear(childrenCount);
        if (childrenCount > 0) {
            int i = 0;
            for (final ChildReference child : node.getChildren()) {
                children.setAt(i++, child);
            }
            children.setSize(childrenCount);
        }
    }

    private byte getLoggableType() {
        byte result = PatriciaTreeBase.NODE_WO_KEY_WO_VALUE_WO_CHILDREN;
        if (hasKey()) {
            result += PatriciaTreeBase.HAS_KEY_BIT;
        }
        if (hasValue()) {
            result += PatriciaTreeBase.HAS_VALUE_BIT;
        }
        if (hasChildren()) {
            result += PatriciaTreeBase.HAS_CHILDREN_BIT;
        }
        if (isRoot()) {
            result += PatriciaTreeBase.ROOT_BIT;
        }
        return result;
    }

    private static final class MutableNodeChildrenIterator implements NodeChildrenIterator {

        @NotNull
        private final MutableNode node;
        @NotNull
        private final ChildReferenceSet.ChildReferenceIterator refs;
        private final ByteIterable key;
        private ChildReference ref;

        private MutableNodeChildrenIterator(@NotNull final MutableNode node, @NotNull final ChildReferenceSet refs) {
            this.node = node;
            this.refs = refs.iterator();
            this.key = node.keySequence;
        }

        private MutableNodeChildrenIterator(@NotNull final MutableNode node,
                                            @NotNull final ChildReferenceSet.ChildReferenceIterator refs) {
            this.node = node;
            this.refs = refs;
            ref = refs.currentRef();
            this.key = node.keySequence;
        }

        @Override
        public boolean hasNext() {
            return refs.hasNext();
        }

        @Override
        public ChildReference next() {
            return ref = refs.next();
        }

        @Override
        public boolean hasPrev() {
            return refs.getIndex() > 0;
        }

        @Override
        public ChildReference prev() {
            return ref = refs.prev();
        }

        @Override
        public boolean isMutable() {
            return true;
        }

        @Override
        public void nextInPlace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void prevInPlace() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChildReference getNode() {
            return ref;
        }

        @Override
        public void remove() {
            node.removeChild(ref.firstByte);
        }

        @Override
        public NodeBase getParentNode() {
            return node;
        }

        @Override
        public int getIndex() {
            return refs.getIndex();
        }

        @Override
        public ByteIterable getKey() {
            return key;
        }
    }
}