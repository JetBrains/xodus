/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.tree.ibtree;

import java.nio.ByteBuffer;

public interface TraversablePage {
    int getEntriesCount();

    TraversablePage child(int index);

    int find(ByteBuffer key);

    boolean isInternalPage();

    ByteBuffer value(int index);

    ByteBuffer key(int index);

    long address();

    int getKeyPrefixSize();

    ByteBuffer keyPrefix();

    default ByteBuffer fullKey(int index) {
        var key = key(index);
        var prefixSize = getKeyPrefixSize();

        if (prefixSize == 0) {
            return key;
        }

        var fullKey = ByteBuffer.allocate(key.limit() + prefixSize);
        var keyPrefix = keyPrefix();

        fullKey.put(0, keyPrefix, 0, prefixSize);
        fullKey.put(prefixSize, key, 0, key.limit());

        return fullKey;
    }

    default ByteBuffer find(ByteBuffer key, ImmutableCursorState state, int depth) {
        var keyPrefixSize = getKeyPrefixSize();

        if (key.limit() < keyPrefixSize) {
            return null;
        }
        key.position(keyPrefixSize);

        var index = find(key);

        if (!isInternalPage()) {
            if (index >= 0) {
                state.setItemAndSize(depth, this, index);
                return value(index);
            }

            return null;
        }

        if (index < 0) {
            index = -index - 2;

            if (index < 0) {
                return null;
            }
        }

        var child = state.getOrClear(depth, index);
        if (child == null) {
            child = child(index);
        }

        var result = child.find(key, state, depth + 1);

        if (result != null) {
            state.set(depth, this, index);
            return result;
        }

        return null;
    }

    default ByteBuffer findByKeyRange(ByteBuffer key, ImmutableCursorState state, int depth, boolean useFirstEntry) {
        var keyPrefixSize = getKeyPrefixSize();
        if (key.limit() < keyPrefixSize) {
            useFirstEntry = true;
        } else {
            key.position(keyPrefixSize);
        }

        int index;
        if (useFirstEntry) {
            index = 0;
        } else {
            index = find(key);
        }

        if (!isInternalPage()) {
            if (index < 0) {
                index = -index - 1;

                var entriesCount = getEntriesCount();
                if (index < entriesCount) {
                    state.setItemAndSize(depth, this, index);
                    return value(index);
                } else {
                    return null;
                }
            } else {
                state.setItemAndSize(depth, this, index);
                return value(index);
            }
        }

        if (index < 0) {
            index = -index - 1;

            if (index > 0) {
                index--;
            } else {
                useFirstEntry = true;
            }
        }

        var child = state.getOrClear(depth, index);
        if (child == null) {
            child = child(index);
        }

        var entriesCount = getEntriesCount();
        while (true) {
            var result = child.findByKeyRange(key, state, depth + 1, useFirstEntry);


            if (result == null) {
                state.clear(depth);
                if (entriesCount > index + 1) {
                    index++;
                    useFirstEntry = true;
                    child = child(index);
                } else {
                    return null;
                }
            } else {
                state.set(depth, this, index);
                return result;
            }
        }
    }
}
