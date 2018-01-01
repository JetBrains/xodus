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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;

import java.util.ConcurrentModificationException;

final class CursorImpl implements Cursor {

    private static final String CANT_DELETE_MODIFIED_MSG = "Can't delete (pair not found in mutable tree)";

    @NotNull
    private final StoreImpl store;
    @NotNull
    private final TransactionBase txn;
    private ITreeCursor treeCursor;
    private volatile boolean isClosed;

    CursorImpl(@NotNull final StoreImpl store, @NotNull final TransactionBase txn) {
        this.store = store;
        this.txn = txn;
        treeCursor = null;
        isClosed = false;
    }

    @Override
    public boolean getNext() {
        checkTreeCursor();
        return treeCursor.getNext();
    }

    @Override
    public boolean getNextDup() {
        checkTreeCursor();
        return treeCursor.getNextDup();
    }

    @Override
    public boolean getNextNoDup() {
        checkTreeCursor();
        return treeCursor.getNextNoDup();
    }

    @Override
    public boolean getPrev() {
        checkTreeCursor();
        return treeCursor.getPrev();
    }

    @Override
    public boolean getPrevDup() {
        checkTreeCursor();
        return treeCursor.getPrevDup();
    }

    @Override
    public boolean getPrevNoDup() {
        checkTreeCursor();
        return treeCursor.getPrevNoDup();
    }

    @Override
    public boolean getLast() {
        checkTreeCursor();
        return treeCursor.getLast();
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        checkTreeCursor();
        return treeCursor.getKey();
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        checkTreeCursor();
        return treeCursor.getValue();
    }

    @Override
    public ByteIterable getSearchKey(@NotNull final ByteIterable key) {
        checkTreeCursor();
        return treeCursor.getSearchKey(key);
    }

    @Override
    public ByteIterable getSearchKeyRange(@NotNull final ByteIterable key) {
        checkTreeCursor();
        return treeCursor.getSearchKeyRange(key);
    }

    @Override
    public boolean getSearchBoth(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        checkTreeCursor();
        return treeCursor.getSearchBoth(key, value);
    }

    @Override
    public ByteIterable getSearchBothRange(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        checkTreeCursor();
        return treeCursor.getSearchBothRange(key, value);
    }

    @Override
    public int count() {
        checkTreeCursor();
        return treeCursor.count();
    }

    @Override
    public boolean isMutable() {
        checkTreeCursor();
        return treeCursor.isMutable();
    }

    @Override
    public void close() {
        if (isClosed) {
            throw new ExodusException("Cursor is already closed");
        }
        if (treeCursor != null) {
            treeCursor.close();
        }
        isClosed = true;
    }

    @Override
    public boolean deleteCurrent() {
        final ReadWriteTransaction txn = EnvironmentImpl.throwIfReadonly(this.txn,
                "Can't delete a key/value pair of cursor in read-only transaction");
        if (treeCursor == null) {
            treeCursor = txn.getMutableTree(store).openCursor();
        } else {
            if (!treeCursor.isMutable()) {
                final ByteIterable key = treeCursor.getKey();
                final ByteIterable value = treeCursor.getValue();
                final ITreeCursor newCursor = txn.getMutableTree(store).openCursor();
                if (newCursor.getSearchBoth(key, value)) {
                    treeCursor = newCursor; // navigated to same pair, ready to delete
                } else {
                    throw new ConcurrentModificationException(CANT_DELETE_MODIFIED_MSG);
                }
            }
        }
        return treeCursor.deleteCurrent();
    }

    private void checkTreeCursor() {
        if (treeCursor == null) {
            treeCursor = txn.getTree(store).openCursor();
        }
    }
}
