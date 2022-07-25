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

import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.DataIterator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;

public final class MutableBTree implements IBTreeMutable {
    final ExpiredLoggableCollection expiredLoggables = new ExpiredLoggableCollection();
    @Nullable
    HashSet<ITreeCursorMutable> cursors;

    final ImmutableBTree immutableTree;
    final Log log;

    @NotNull
    MutablePage root;

    public MutableBTree(ImmutableBTree immutableTree) {
        this.immutableTree = immutableTree;
        this.log = immutableTree.log;

        var immutableRoot = immutableTree.root;
        if (root == null) {
            this.root = new MutableLeafPage(this, null, log, log.getCachePageSize(),
                    expiredLoggables, null);
        } else {
            this.root = immutableRoot.toMutable(this, expiredLoggables, null);
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
        return root.address();
    }

    @Override
    public int getStructureId() {
        return immutableTree.structureId;
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
        var cursor = new TreeMutableCursor(this, this.root);

        if (cursors == null) {
            cursors = new HashSet<>();
        }

        cursors.add(cursor);

        return cursor;
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
        return cursors;
    }

    @Override
    public void cursorClosed(@NotNull ITreeCursorMutable cursor) {
        assert cursors != null;

        cursors.remove(cursor);
    }

    @Override
    public boolean put(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var bufferKey = key.getByteBuffer();

        var page = root;

        while (true) {
            var index = page.find(bufferKey);
            if (page instanceof MutableLeafPage mutablePage) {
                if (index < 0) {
                    mutablePage.insert(-index - 1, bufferKey, value.getByteBuffer());

                    TreeMutableCursor.notifyCursors(this);
                    return true;
                }

                mutablePage.set(index, bufferKey, value.getByteBuffer());

                TreeMutableCursor.notifyCursors(this);
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

                TreeMutableCursor.notifyCursors(this);
                return;
            } else {
                var mutableInternalPage = (MutableInternalPage) page;
                var numChildren = mutableInternalPage.getEntriesCount();

                var mutableChild = mutableInternalPage.mutableChildIfExists(numChildren - 1);
                if (mutableChild == null) {
                    assert mutableInternalPage.underlying != null;

                    var immutableChildAddress =
                            mutableInternalPage.underlying.getChildAddress(numChildren - 1);
                    var immutableChild = immutableTree.loadPage(immutableChildAddress);
                    mutableChild = immutableChild.toMutable(this, expiredLoggables, mutableInternalPage);
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

                    TreeMutableCursor.notifyCursors(this);
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
        var result = doDelete(key.getByteBuffer(), null);

        if (result) {
            TreeMutableCursor.notifyCursors(this);
        }

        return result;
    }

    @Override
    public boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value,
                          @Nullable ITreeCursorMutable cursorToSkip) {
        ByteBuffer bufferValue;
        if (value != null) {
            bufferValue = value.getByteBuffer();
        } else {
            bufferValue = null;
        }

        if (doDelete(key.getByteBuffer(), bufferValue)) {
            TreeMutableCursor.notifyCursors(this, cursorToSkip);
        }

        return false;
    }

    private boolean doDelete(final ByteBuffer key, final ByteBuffer value) {
        var page = root;
        while (true) {
            var index = page.find(key);
            if (page instanceof MutableLeafPage mutablePage) {
                if (index > 0) {
                    if (value == null) {
                        mutablePage.delete(index);
                        return true;
                    }

                    var val = page.getValue(index);
                    if (ByteBufferComparator.INSTANCE.compare(val, value) == 0) {
                        mutablePage.delete(index);
                        return true;
                    }

                    return false;
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
    public long save() {
        var newRoot = root.rebalance();
        if (newRoot != null) {
            root = newRoot;
        }

        root.spill();

        return root.save(immutableTree.getStructureId());
    }

    @Override
    public @NotNull ExpiredLoggableCollection getExpiredLoggables() {
        return expiredLoggables;
    }

    @Override
    public boolean reclaim(@NotNull RandomAccessLoggable loggable, @NotNull Iterator<RandomAccessLoggable> loggables,
                           long segmentSize) {
        return false;
    }

    @Override
    public @NotNull TraversablePage getRoot() {
        return root;
    }
}
