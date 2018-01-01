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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Leaf node for external usage.
 */
public class LeafNodeKV extends BaseLeafNode {
    protected ByteIterable key;
    protected ByteIterable value;

    public LeafNodeKV(@NotNull ByteIterable key, @Nullable ByteIterable value) {
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
    public boolean isMutable() {
        return false;
    }

    @Override
    public String toString() {
        return "KV {key:" + getKey().toString() + "} @ " + getAddress();
    }
}
