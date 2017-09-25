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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.TreeCursor;
import org.jetbrains.annotations.NotNull;

final class BTreeDup extends BTreeBase {

    @NotNull
    final LeafNodeDup leafNodeDup;
    private final ByteIterable leafNodeDupKey;
    private final long startAddress;
    private final int dataOffset;

    BTreeDup(@NotNull BTreeBase mainTree, @NotNull LeafNodeDup leafNodeDup) {
        super(mainTree.getLog(), mainTree.getBalancePolicy(), false, mainTree.getStructureId());
        this.leafNodeDup = leafNodeDup;
        leafNodeDupKey = leafNodeDup.getKey();
        final ByteIterator iterator = leafNodeDup.getRawValue(0).iterator();
        final long l = CompressedUnsignedLongByteIterable.getLong(iterator);
        size = l >> 1;
        if ((l & 1) == 1) {
            long offset = CompressedUnsignedLongByteIterable.getLong(iterator);
            startAddress = leafNodeDup.getAddress() - offset;
            dataOffset = CompressedUnsignedLongByteIterable.getCompressedSize(l)
                    + CompressedUnsignedLongByteIterable.getCompressedSize(offset);
        } else {
            startAddress = Loggable.NULL_ADDRESS;
            dataOffset = CompressedUnsignedLongByteIterable.getCompressedSize(l);
        }
    }

    @Override
    public long getRootAddress() {
        throw new UnsupportedOperationException("BTreeDup has no root in 'Loggable' terms");
    }

    public long getStartAddress() {
        return startAddress;
    }

    @Override
    @NotNull
    public BTreeDupMutable getMutableCopy() {
        return new BTreeDupMutable(this, leafNodeDupKey);
    }

    @Override
    public TreeCursor openCursor() {
        return new TreeCursor(new BTreeTraverser(getRoot()));
    }

    @NotNull
    @Override
    protected BasePage getRoot() {
        return loadPage(leafNodeDup.getType(), leafNodeDup.getRawValue(dataOffset));
    }

    @Override
    @NotNull
    protected LeafNode loadLeaf(final long address) {
        final RandomAccessLoggable loggable = getLoggable(address);
        if (loggable.getType() == DUP_LEAF) {
            return new LeafNode(loggable) {
                @NotNull
                @Override
                public ByteIterable getValue() {
                    return leafNodeDupKey; // get key from tree
                }

                @Override
                public boolean isDupLeaf() {
                    return true;
                }

                @Override
                public String toString() {
                    return "DLN {key:" + getKey().toString() + "} @ " + getAddress();
                }
            };
        } else {
            throw new IllegalArgumentException("Unexpected loggable type " + loggable.getType() + " at address " + loggable.getAddress());
        }
    }
}
