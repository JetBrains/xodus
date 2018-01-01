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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.ExpiredLoggableInfo;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.*;
import jetbrains.exodus.util.ByteIterableUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Iterator;

final class PatriciaTreeWithDuplicatesMutable extends PatriciaTreeWithDuplicates implements ITreeMutable {
    PatriciaTreeWithDuplicatesMutable(@NotNull final ITreeMutable treeNoDuplicates) {
        super(treeNoDuplicates);
    }

    @NotNull
    @Override
    public ITreeMutable getMutableCopy() {
        return this;
    }

    @Override
    public void cursorClosed(@NotNull ITreeCursorMutable cursor) {
        throw new UnsupportedOperationException();
        // ((PatriciaCursorDecorator)cursor).patriciaCursor.close();
    }

    @Override
    public MutableTreeRoot getRoot() {
        return getTreeNoDuplicates().getRoot();
    }

    @Override
    public boolean isAllowingDuplicates() {
        return true;
    }

    @Override
    @Nullable
    public Iterable<ITreeCursorMutable> getOpenCursors() {
        return getTreeNoDuplicates().getOpenCursors();
    }

    @Override
    public boolean put(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return getTreeNoDuplicates().put(
                getEscapedKeyValue(key, value), CompressedUnsignedLongByteIterable.getIterable(key.getLength()));
    }

    @Override
    public void putRight(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        getTreeNoDuplicates().putRight(
                getEscapedKeyValue(key, value), CompressedUnsignedLongByteIterable.getIterable(key.getLength()));
    }

    @Override
    public boolean add(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return getTreeNoDuplicates().add(
                getEscapedKeyValue(key, value), CompressedUnsignedLongByteIterable.getIterable(key.getLength()));
    }

    @Override
    public void put(@NotNull final INode ln) {
        put(ln.getKey(), PatriciaTreeMutable.getNotNullValue(ln));
    }

    @Override
    public void putRight(@NotNull final INode ln) {
        putRight(ln.getKey(), PatriciaTreeMutable.getNotNullValue(ln));
    }

    @Override
    public boolean add(@NotNull final INode ln) {
        return add(ln.getKey(), PatriciaTreeMutable.getNotNullValue(ln));
    }

    @Override
    public boolean delete(@NotNull final ByteIterable key) {
        boolean wasDeleted = false;
        try (ITreeCursor cursor = treeNoDuplicates.openCursor()) {
            final byte[] keyBytes = key.getBytesUnsafe();
            final int keyLength = key.getLength();
            @Nullable
            ByteIterable value = cursor.getSearchKeyRange(getEscapedKeyWithSeparator(key));
            while (value != null) {
                if (keyLength != CompressedUnsignedLongByteIterable.getInt(value)) {
                    break;
                }
                final ByteIterable noDupKey = new UnEscapingByteIterable(cursor.getKey());
                if (ByteIterableUtil.compare(keyBytes, keyLength, noDupKey.getBytesUnsafe(), keyLength) != 0) {
                    break;
                }
                cursor.deleteCurrent();
                wasDeleted = true;
                value = cursor.getNext() ? cursor.getValue() : null;
            }
        }
        return wasDeleted;
    }

    @Override
    public boolean delete(@NotNull final ByteIterable key,
                          @Nullable final ByteIterable value,
                          @Nullable final ITreeCursorMutable cursorToSkip) {
        if (value == null) {
            return delete(key);
        }
        if (getTreeNoDuplicates().delete(getEscapedKeyValue(key, value))) {
            TreeCursorMutable.notifyCursors(this, cursorToSkip);
            return true;
        }
        return false;
    }

    @Override
    public long save() {
        return getTreeNoDuplicates().save();
    }

    @NotNull
    @Override
    public Collection<ExpiredLoggableInfo> getExpiredLoggables() {
        return getTreeNoDuplicates().getExpiredLoggables();
    }

    @Override
    public boolean reclaim(@NotNull final RandomAccessLoggable loggable,
                           @NotNull final Iterator<RandomAccessLoggable> loggables) {
        return ((ITreeMutable) treeNoDuplicates).reclaim(loggable, loggables);
    }

    private ITreeMutable getTreeNoDuplicates() {
        return (ITreeMutable) treeNoDuplicates;
    }
}
