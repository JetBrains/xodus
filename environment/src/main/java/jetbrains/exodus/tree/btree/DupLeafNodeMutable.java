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
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ITree;
import org.jetbrains.annotations.NotNull;

/**
 * Stateful leaf node for mutable tree of duplicates
 */
class DupLeafNodeMutable extends BaseLeafNodeMutable {

    protected long address = Loggable.NULL_ADDRESS;
    protected final ByteIterable key;
    protected final BTreeDupMutable dupTree;

    DupLeafNodeMutable(@NotNull ByteIterable key, @NotNull BTreeDupMutable dupTree) {
        this.key = key;
        this.dupTree = dupTree;
    }

    @Override
    public boolean isDupLeaf() {
        return true;
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        return key;
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        return dupTree.key;
    }

    @Override
    public long getAddress() {
        return address;
    }

    @Override
    public long save(ITree mainTree) {
        assert mainTree == dupTree;
        if (address != Loggable.NULL_ADDRESS) {
            throw new IllegalStateException("Leaf already saved");
        }
        address = mainTree.getLog().write(((BTreeMutable) mainTree).getLeafType(),
                mainTree.getStructureId(),
                new CompoundByteIterable(new ByteIterable[]{
                        CompressedUnsignedLongByteIterable.getIterable(key.getLength()),
                        key
                }));
        return address;
    }

    @Override
    public boolean delete(ByteIterable value) {
        throw new UnsupportedOperationException("Supported by dup node only");
    }

    @Override
    public String toString() {
        return "DLN* {key:" + getKey().toString() + "} @ " + getAddress();
    }
}
