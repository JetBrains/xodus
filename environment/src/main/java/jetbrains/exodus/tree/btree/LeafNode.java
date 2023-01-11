/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.*;
import org.jetbrains.annotations.NotNull;

/**
 * Stateless leaf node for immutable btree
 */
class LeafNode extends BaseLeafNode {

    @NotNull
    private final RandomAccessLoggable loggable;
    private final int keyLength;
    protected final Log log;
    protected final boolean insideSinglePage;

    LeafNode(final Log log, @NotNull final RandomAccessLoggable loggable) {
        this.log = log;
        this.loggable = loggable;
        final ByteIterableWithAddress data = loggable.getData();
        final int keyLength = data.getCompressedUnsignedInt();
        final int keyRecordSize = CompressedUnsignedLongByteIterable.getCompressedSize(keyLength);
        this.keyLength = (keyLength << 3) + keyRecordSize;
        this.insideSinglePage = loggable.isDataInsideSinglePage();
    }

    @Override
    public long getAddress() {
        return loggable.getAddress();
    }

    public int getType() {
        return loggable.getType();
    }

    @Override
    public int compareKeyTo(@NotNull final ByteIterable iterable) {
        return loggable.getData().compareTo(getKeyRecordSize(), getKeyLength(), iterable,
                0, iterable.getLength());
    }

    @Override
    public int compareValueTo(@NotNull final ByteIterable iterable) {
        return loggable.getData().compareTo(getKeyRecordSize() + getKeyLength(),
                getValueLength(), iterable, 0, iterable.getLength());
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        return loggable.getData().subIterable(getKeyRecordSize(), getKeyLength());
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        int valueLength = getValueLength();
        if (valueLength == 0) {
            return ArrayByteIterable.EMPTY;
        }
        return loggable.getData().subIterable(getKeyRecordSize() + getKeyLength(), valueLength);
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    Loggable getLoggable() {
        return loggable;
    }

    @NotNull
    ByteIterableWithAddress getRawValue(final int offset) {
        final var data = loggable.getData();
        return data.cloneWithOffset(getKeyRecordSize() + getKeyLength() + offset);
    }

    private int getKeyLength() {
        return keyLength >>> 3;
    }

    private int getKeyRecordSize() {
        return keyLength & 7;
    }

    private int getValueLength() {
        return loggable.getDataLength() - getKeyRecordSize() - getKeyLength();
    }

    protected void doReclaim(@NotNull BTreeReclaimTraverser context, final int leafIndex) {
        final long keyAddress = context.currentNode.getKeyAddress(leafIndex);
        if (keyAddress == loggable.getAddress()) {
            final BTreeMutable tree = context.mainTree;
            tree.addExpiredLoggable(keyAddress);
            final BasePageMutable node = context.currentNode.getMutableCopy(tree);
            node.set(leafIndex, tree.createMutableLeaf(getKey(), getValue()), null);
            context.wasReclaim = true;
            context.setPage(node);
        }
    }

    protected void reclaim(@NotNull final BTreeReclaimTraverser context) {
        final ByteIterable keyIterable = getKey();
        if (!context.canMoveDown() && context.canMoveRight()) {
            final int leafIndex;
            final int cmp = context.compareCurrent(keyIterable);
            if (cmp > 0) {
                return;
            }
            if (cmp == 0) {
                leafIndex = context.currentPos;
            } else {
                context.moveRight();
                leafIndex = context.getNextSibling(keyIterable, loggable.getAddress());
            }
            if (leafIndex >= 0) {
                doReclaim(context, leafIndex);
                context.moveTo(leafIndex + 1);
                return;
            } else if (context.canMoveTo(-leafIndex - 1)) {
                return;
            }
        }
        // go up
        if (context.canMoveUp()) {
            while (true) {
                context.popAndMutate();
                context.moveRight();
                final int index = context.getNextSibling(keyIterable);
                if (index < 0) {
                    if (context.canMoveTo(-index - 1) || !context.canMoveUp()) {
                        context.moveTo(Math.max(-index - 2, 0));
                        break;
                    }
                } else {
                    context.pushChild(index); // node is always internal
                    break;
                }
            }
        }
        // go down
        while (context.canMoveDown()) {
            int index = context.getNextSibling(keyIterable);
            if (index < 0) {
                index = Math.max(-index - 2, 0);
            }
            context.pushChild(index);
        }
        int leafIndex = context.getNextSibling(keyIterable);
        if (leafIndex >= 0) {
            doReclaim(context, leafIndex);
            context.moveTo(leafIndex + 1);
        } else {
            context.moveTo(-leafIndex - 1);
        }
    }
}
