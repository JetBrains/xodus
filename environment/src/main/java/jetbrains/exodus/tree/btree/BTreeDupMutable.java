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
import jetbrains.exodus.log.*;
import org.jetbrains.annotations.NotNull;

final class BTreeDupMutable extends BTreeMutable {

    BTreeMutable mainTree;

    @NotNull
    ByteIterable key;
    long address = Loggable.NULL_ADDRESS;

    BTreeDupMutable(@NotNull final BTreeBase dupTree, @NotNull final ByteIterable key) {
        super(dupTree);
        size = dupTree.size;
        this.key = key;
    }

    @Override
    protected void addExpiredLoggable(@NotNull Loggable loggable) {
        mainTree.addExpiredLoggable(loggable);
    }

    @Override
    protected void addExpiredLoggable(long address) {
        mainTree.addExpiredLoggable(address);
    }

    @Override
    protected void decrementSize(final long delta) {
        super.decrementSize(delta);
        mainTree.decrementSize(delta);
    }

    @Override
    protected void incrementSize() {
        super.incrementSize();
        mainTree.incrementSize();
    }

    @Override
    public long save() {
        if (address != Loggable.NULL_ADDRESS) {
            throw new IllegalStateException("Duplicates sub-tree already saved");
        }
        final BasePageMutable rootPage = getRoot();
        final byte type = rootPage.isBottom() ? BTreeBase.LEAF_DUP_BOTTOM_ROOT : BTreeBase.LEAF_DUP_INTERNAL_ROOT;
        final ByteIterable keyIterable = CompressedUnsignedLongByteIterable.getIterable(key.getLength());
        ByteIterable sizeIterable;
        long startAddress = log.getHighAddress(); // remember high address before saving the data
        final ByteIterable rootDataIterable = rootPage.getData();
        ByteIterable[] iterables;
        long result;
        final boolean canRetry;
        if (log.isLastFileAddress(startAddress)) {
            sizeIterable = CompressedUnsignedLongByteIterable.getIterable(size << 1);
            iterables = new ByteIterable[]{keyIterable, key, sizeIterable, rootDataIterable};
            result = log.tryWrite(type, structureId, new CompoundByteIterable(iterables));
            if (result >= 0) {
                address = result;
                return result;
            } else {
                canRetry = false;
            }
        } else {
            canRetry = true;
        }
        if (NullLoggable.isNullLoggable(log.read(startAddress))) {
            final long lengthBound = log.getFileLengthBound();
            startAddress += (lengthBound - startAddress % lengthBound);
        }
        sizeIterable = CompressedUnsignedLongByteIterable.getIterable((size << 1) + 1);
        final ByteIterable offsetIterable =
            CompressedUnsignedLongByteIterable.getIterable(log.getHighAddress() - startAddress);
        iterables = new ByteIterable[]{keyIterable, key, sizeIterable, offsetIterable, rootDataIterable};
        final ByteIterable data = new CompoundByteIterable(iterables);
        result = canRetry ? log.tryWrite(type, structureId, data) : log.writeContinuously(type, structureId, data);
        if (result < 0) {
            if (canRetry) {
                iterables[3] = CompressedUnsignedLongByteIterable.getIterable(log.getHighAddress() - startAddress);
                result = log.writeContinuously(type, structureId, new CompoundByteIterable(iterables));

                if (result < 0) {
                    throw new TooBigLoggableException();
                }
            } else {
                throw new TooBigLoggableException();
            }
        }
        address = result;
        return result;
    }

    @Override
    protected byte getBottomPageType() {
        return DUP_BOTTOM;
    }

    @Override
    protected byte getInternalPageType() {
        return DUP_INTERNAL;
    }

    @Override
    protected byte getLeafType() {
        return DUP_LEAF;
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
                    return BTreeDupMutable.this.key; // get key from tree
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

    @Override
    @NotNull
    protected BaseLeafNodeMutable createMutableLeaf(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return new DupLeafNodeMutable(key, this);
    }
}
