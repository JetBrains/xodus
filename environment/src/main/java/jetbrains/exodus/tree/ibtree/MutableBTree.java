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
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.ByteBufferIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;

public final class MutableBTree implements ITreeMutable {
    final ExpiredLoggableCollection expiredLoggables = new ExpiredLoggableCollection();

    final ImmutableBTree immutableTree;
    final Log log;
    MutablePage root;

    public MutableBTree(ImmutableBTree immutableTree) {
        this.immutableTree = immutableTree;
        this.log = immutableTree.log;

        var immutableRoot = immutableTree.root;
        if (root == null) {
            this.root = new MutableLeafPage(null, log, log.getCachePageSize(), expiredLoggables, null);
        } else {
            this.root = immutableRoot.toMutable(expiredLoggables, null);
        }
    }

    @Override
    public @NotNull Log getLog() {
        return log;
    }

    @Override
    public @NotNull DataIterator getDataIterator(long address) {
        return immutableTree.getDataIterator(address);
    }

    @Override
    public long getRootAddress() {
        return Loggable.NULL_ADDRESS;
    }

    @Override
    public int getStructureId() {
        return immutableTree.structureId;
    }

    @Override
    public @Nullable ByteIterable get(@NotNull ByteIterable key) {
        var bufferKey = key.getByteBuffer();
        var pageIndexPair = findPair(bufferKey);
        if (pageIndexPair == null) {
            return null;
        }

        return new ByteBufferByteIterable(pageIndexPair.page.valueView.get(pageIndexPair.entryIndex));
    }

    private PageElementIndexPair findPair(final ByteBuffer key) {
        var page = root;

        while (true) {
            var index = page.find(key);
            if (page instanceof MutableLeafPage) {
                if (index < 0) {
                    return null;
                }

                return new PageElementIndexPair((MutableLeafPage) page, index);
            } else {
                if (index < 0) {
                    index = -index - 2;

                    if (index < 0) {
                        return null;
                    }
                }

                var internalPage = (MutableInternalPage) page;
                var mutablePage = internalPage.mutableChildIfExists(index);

                if (mutablePage == null) {
                    assert internalPage.underlying != null;
                    var childAddress = internalPage.underlying.getChildAddress(index);
                    var immutablePage = immutableTree.loadPage(childAddress);
                    mutablePage = immutablePage.toMutable(expiredLoggables, internalPage);
                }

                page = mutablePage;
            }
        }
    }

    @Override
    public boolean hasPair(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var bufferKey = key.getByteBuffer();
        var pageIndexPair = findPair(bufferKey);

        if (pageIndexPair == null) {
            return false;
        }

        var foundValue = pageIndexPair.page.valueView.get(pageIndexPair.entryIndex);

        if (value instanceof ByteBufferIterable) {
            var bufferValue = value.getByteBuffer();
            return ByteBufferComparator.INSTANCE.compare(foundValue, bufferValue) == 0;
        }

        return new ByteBufferByteIterable(foundValue).compareTo(value) == 0;
    }

    @Override
    public boolean hasKey(@NotNull ByteIterable key) {
        var bufferKey = key.getByteBuffer();
        var pageIndexPair = findPair(bufferKey);

        return pageIndexPair != null;
    }

    @Override
    public @NotNull ITreeMutable getMutableCopy() {
        return this;
    }

    @Override
    public boolean isEmpty() {
        return root.treeSize() == 0;
    }

    @Override
    public long getSize() {
        return root.treeSize();
    }

    @Override
    public ITreeCursor openCursor() {
        return null;
    }

    @Override
    public LongIterator addressIterator() {
        return immutableTree.addressIterator();
    }


    @Override
    public boolean isAllowingDuplicates() {
        return false;
    }

    @Override
    public @Nullable Iterable<ITreeCursorMutable> getOpenCursors() {
        return null;
    }

    @Override
    public void cursorClosed(@NotNull ITreeCursorMutable cursor) {
    }

    @Override
    public boolean put(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var bufferKey = key.getByteBuffer();

        var page = root;

        while (true) {
            var index = page.find(bufferKey);
            if (page instanceof MutableLeafPage mutablePage) {
                if (index < 0) {
                    mutablePage.insert(index, bufferKey, value.getByteBuffer());
                    return true;
                }

                mutablePage.set(index, bufferKey, value.getByteBuffer());

                return true;
            } else {
                if (index < 0) {
                    index = -index - 1;

                    if (index > 0) {
                        index--;
                    }
                }

                var internalPage = (MutableInternalPage) page;
                page = internalPage.mutableChild(index);
            }
        }
    }

    @Override
    public void putRight(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var page = root;

        while (true) {
            if (page instanceof MutableLeafPage mutableLeafPage) {
                mutableLeafPage.append(key.getByteBuffer(), value.getByteBuffer());
                return;
            } else {
                var mutableInternalPage = (MutableInternalPage) page;
                var numChildren = mutableInternalPage.numChildren();

                var mutableChild = mutableInternalPage.mutableChildIfExists(numChildren - 1);
                if (mutableChild == null) {
                    assert mutableInternalPage.underlying != null;

                    var immutableChildAddress =
                            mutableInternalPage.underlying.getChildAddress(numChildren - 1);
                    var immutableChild = immutableTree.loadPage(immutableChildAddress);
                    mutableChild = immutableChild.toMutable(expiredLoggables, mutableInternalPage);
                }

                page = mutableChild;
            }
        }
    }

    @Override
    public boolean add(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var bufferKey = key.getByteBuffer();

        var page = root;

        while (true) {
            var index = page.find(bufferKey);
            if (page instanceof MutableLeafPage mutablePage) {
                if (index < 0) {
                    mutablePage.insert(index, bufferKey, value.getByteBuffer());
                    return true;
                }

                return false;
            } else {
                if (index < 0) {
                    index = -index - 1;

                    if (index > 0) {
                        index--;
                    }
                }

                var internalPage = (MutableInternalPage) page;
                page = internalPage.mutableChild(index);
            }
        }
    }

    @Override
    public void put(@NotNull INode ln) {
        var value = ln.getValue();
        var key = ln.getKey();

        Objects.requireNonNull(value);

        put(key, value);
    }

    @Override
    public void putRight(@NotNull INode ln) {
        var value = ln.getValue();
        var key = ln.getKey();

        Objects.requireNonNull(value);

        putRight(key, value);
    }

    @Override
    public boolean add(@NotNull INode ln) {
        var value = ln.getValue();
        var key = ln.getKey();

        Objects.requireNonNull(value);
        return add(key, value);
    }

    @Override
    public boolean delete(@NotNull ByteIterable key) {
        var bufferKey = key.getByteBuffer();

        var page = root;

        while (true) {
            var index = page.find(bufferKey);
            if (page instanceof MutableLeafPage mutablePage) {
                if (index > 0) {
                    mutablePage.delete(index);
                    return true;
                }

                return false;
            } else {
                if (index < 0) {
                    index = -index - 1;

                    if (index > 0) {
                        index--;
                    } else {
                        return false;
                    }
                }

                var internalPage = (MutableInternalPage) page;
                page = internalPage.mutableChild(index);
            }
        }
    }

    @Override
    public boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value,
                          @Nullable ITreeCursorMutable cursorToSkip) {
        return false;
    }

    @Override
    public long save() {
        root.rebalance();
        root.spill();

        return root.save(immutableTree.getStructureId());
    }

    @Override
    public @NotNull ExpiredLoggableCollection getExpiredLoggables() {
        return expiredLoggables;
    }

    @Override
    public boolean reclaim(@NotNull RandomAccessLoggable loggable, @NotNull Iterator<RandomAccessLoggable> loggables) {
        return false;
    }

    @SuppressWarnings("ClassCanBeRecord")
    private static final class PageElementIndexPair {
        private final MutableLeafPage page;
        private final int entryIndex;

        private PageElementIndexPair(MutableLeafPage page, int entryIndex) {
            this.page = page;
            this.entryIndex = entryIndex;
        }
    }
}
