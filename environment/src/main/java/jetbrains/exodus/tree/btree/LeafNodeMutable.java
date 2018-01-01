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
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;

/**
 * Stateful leaf node for mutable tree
 */
class LeafNodeMutable extends BaseLeafNodeMutable {

    private long address = Loggable.NULL_ADDRESS;
    private final ByteIterable key;
    private final ByteIterable value;

    LeafNodeMutable(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        this.key = key;
        this.value = value;
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        return key;
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        return value;
    }

    @Override
    public long getAddress() {
        return address;
    }

    @Override
    public long save(ITree tree) {
        if (address != Loggable.NULL_ADDRESS) {
            throw new IllegalStateException("Leaf already saved");
        }
        final int keyLength = key.getLength();
        final BTreeMutable mutableTree = (BTreeMutable) tree;
        final LightOutputStream output = mutableTree.getLeafStream();
        output.clear();
        CompressedUnsignedLongByteIterable.fillBytes(keyLength, output);
        ByteIterableBase.fillBytes(key, output);
        ByteIterableBase.fillBytes(value, output);
        address = tree.getLog().write(mutableTree.getLeafType(), tree.getStructureId(), output.asArrayByteIterable());
        return address;
    }

    @Override
    public boolean delete(ByteIterable value) {
        throw new UnsupportedOperationException("Supported by dup node only");
    }

    @Override
    public String toString() {
        return "LN* {key:" + key.toString() + "} @ " + address;
    }
}
