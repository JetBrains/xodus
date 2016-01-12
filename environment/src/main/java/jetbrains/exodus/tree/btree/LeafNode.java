/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import jetbrains.exodus.log.ByteIterableWithAddress;
import jetbrains.exodus.log.ByteIteratorWithAddress;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.RandomAccessLoggable;
import org.jetbrains.annotations.NotNull;

/**
 * Stateless leaf node for immutable btree
 */
class LeafNode extends BaseLeafNode {

    @NotNull
    private final RandomAccessLoggable loggable;
    private final int keyLength;
    private final int valueLength;
    private final byte keyRecordSize;

    LeafNode(@NotNull final RandomAccessLoggable loggable) {
        this.loggable = loggable;
        final ByteIterableWithAddress data = loggable.getData();
        final ByteIteratorWithAddress iterator = data.iterator();
        keyLength = CompressedUnsignedLongByteIterable.getInt(iterator);
        final long dataAddress = iterator.getAddress();
        keyRecordSize = (byte) (dataAddress - data.getDataAddress());  //CompressedUnsignedLongByteIterable.getCompressedSize(keyLength);
        valueLength = loggable.getDataLength() - keyRecordSize - keyLength;
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
        return loggable.getData().compareTo(keyRecordSize, keyLength, iterable);
    }

    @Override
    public int compareValueTo(@NotNull final ByteIterable iterable) {
        return loggable.getData().compareTo(keyRecordSize + keyLength, valueLength, iterable);
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        return loggable.getData().subIterable(keyRecordSize, keyLength);
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        return getRawValue().subIterable(0, valueLength);
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @NotNull
    ByteIterableWithAddress getRawValue() {
        return loggable.getData().clone(keyRecordSize + keyLength);
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
                leafIndex = context.getNextSibling(keyIterable).index;
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
                final int index = context.getNextSibling(keyIterable).index;
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
            int index = context.getNextSibling(keyIterable).index;
            if (index < 0) {
                index = Math.max(-index - 2, 0);
            }
            context.pushChild(index);
        }
        int leafIndex = context.getNextSibling(keyIterable).index;
        if (leafIndex >= 0) {
            doReclaim(context, leafIndex);
            context.moveTo(leafIndex + 1);
        } else {
            context.moveTo(-leafIndex - 1);
        }
    }
}
