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

import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteBufferIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITree;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

interface BTree extends ITree {
    @Nullable
    TraversablePage getRoot();

    @Nullable
    default ByteIterable get(@NotNull ByteIterable key) {
        var pageIndexPair = find(key.getByteBuffer());

        if (pageIndexPair == null) {
            return null;
        }

        return new ByteBufferByteIterable(pageIndexPair.page.getValue(pageIndexPair.index));
    }

    default ElemRef find(ByteBuffer key) {
        var root = getRoot();
        if (root == null) {
            return null;
        }

        var page = root;
        while (true) {
            int index = page.find(key);

            if (!page.isInternalPage()) {
                if (index < 0) {
                    return null;
                }

                return new ElemRef(page, index);
            }

            //there is no exact match of the key
            if (index < 0) {
                //index of the first page which contains all keys which are bigger than current one
                index = -index - 1;

                if (index > 0) {
                    index--;
                } else {
                    //all keys in the tree bigger than provided
                    return null;
                }
            }

            page = page.child(index);
        }
    }


    default boolean hasPair(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var elemRef = find(key.getByteBuffer());
        if (elemRef == null) {
            return false;
        }

        var pageValue = elemRef.page.getValue(elemRef.index);
        if (value instanceof ByteBufferIterable) {
            return value.getByteBuffer().compareTo(pageValue) == 0;
        }

        return new ByteBufferByteIterable(pageValue).compareTo(value) == 0;
    }


    default boolean hasKey(@NotNull ByteIterable key) {
        return find(key.getByteBuffer()) != null;
    }
}
