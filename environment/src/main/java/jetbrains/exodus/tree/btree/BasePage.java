/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.Dumpable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

/**
 * BTree base implementation of page
 */
abstract class BasePage implements Dumpable {

    @NotNull
    protected final BTreeBase tree;
    protected int size;

    protected BasePage(@NotNull final BTreeBase tree) {
        this.tree = tree;
    }

    protected final int getSize() {
        return size;
    }

    @NotNull
    protected final BTreeBase getTree() {
        return tree;
    }

    @NotNull
    protected BasePage getChild(int index) {
        throw new UnsupportedOperationException();
    }

    @NotNull ILeafNode getMinKey() {
        if (size <= 0) {
            throw new ArrayIndexOutOfBoundsException("Page is empty.");
        }

        return getKey(0);
    }

    @NotNull ILeafNode getMaxKey() {
        if (size <= 0) {
            throw new ArrayIndexOutOfBoundsException("Page is empty.");
        }

        return getKey(size - 1);
    }

    boolean isInPageRange(@NotNull final ByteIterable key, @Nullable final ByteIterable value) {
        final ILeafNode maxKey = getMaxKey();
        int cmp = maxKey.getKey().compareTo(key);
        if (cmp < 0) {
            return false;
        }
        if (cmp == 0 && value != null) {
            @Nullable final ByteIterable maxValue = maxKey.getValue();
            if (maxValue == null || maxValue.compareTo(value) < 0) {
                return false;
            }
        }
        if (size == 1) {
            return true;
        }
        final ILeafNode minKey = getMinKey();
        cmp = minKey.getKey().compareTo(key);
        if (cmp > 0) {
            return false;
        }
        if (cmp == 0 && value != null) {
            @Nullable final ByteIterable minValue = minKey.getValue();
            return minValue != null && minValue.compareTo(value) <= 0;
        }
        return true;
    }

    protected boolean isDupKey(int index) {
        return getKey(index).isDup();
    }

    @NotNull
    protected abstract BaseLeafNode getKey(int index);

    @NotNull
    protected abstract BasePageMutable getMutableCopy(BTreeMutable treeMutable);

    protected abstract long getDataAddress();

    protected abstract long getKeyAddress(int index);

    protected abstract boolean isBottom();

    protected abstract boolean isMutable();

    protected abstract long getBottomPagesCount();

    protected abstract int binarySearch(final ByteIterable key);

    protected abstract int binarySearch(final ByteIterable key, final int low);

    protected abstract int binarySearch(final ByteIterable key, final int low, final long expectedAddress);

    @Nullable
    protected abstract ILeafNode get(@NotNull final ByteIterable key);

    @Nullable
    protected abstract ILeafNode find(@NotNull BTreeTraverser stack, int depth,
                                      @NotNull ByteIterable key, @Nullable ByteIterable value, boolean equalOrNext);

    protected abstract boolean keyExists(@NotNull ByteIterable key);

    protected abstract boolean exists(@NotNull ByteIterable key, @NotNull ByteIterable value);

    protected abstract boolean childExists(@NotNull ByteIterable key, long pageAddress);

    protected abstract long getChildAddress(int index);

    static void indent(PrintStream out, int level) {
        for (int i = 0; i < level; i++) out.print(" ");
    }
}
