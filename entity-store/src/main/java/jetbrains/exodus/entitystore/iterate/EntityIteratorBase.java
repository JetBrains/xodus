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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.env.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;

@SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod"})
public abstract class EntityIteratorBase implements EntityIterator {

    public static final EntityIteratorBase EMPTY;

    static {
        EMPTY = new NonDisposableEntityIterator(EntityIterableBase.EMPTY) {

            @Override
            public boolean skip(int number) {
                return false;
            }

            protected boolean hasNextImpl() {
                return false;
            }

            @Override
            @Nullable
            public EntityId nextIdImpl() {
                return null;
            }
        };
    }

    private static int nextIdCounter = 0;

    @NotNull
    private final EntityIterableBase iterable;
    private boolean finished;
    private boolean disposed;
    private Cursor cursor;
    @Nullable
    private QueryCancellingPolicy queryCancellingPolicy;

    protected EntityIteratorBase(@NotNull final EntityIterableBase iterable) {
        this.iterable = iterable;
        cursor = null;
        finished = iterable == EntityIterableBase.EMPTY;
        disposed = false;
    }

    @NotNull
    public EntityIterableBase getIterable() {
        return iterable;
    }

    @Override
    public final boolean hasNext() {
        try {
            if (finished) {
                return false;
            }
            checkDisposed();
            final boolean result = hasNextImpl();
            if (!result) {
                finished = true;
                disposeIfShouldBe();
            }
            return result;
        } catch (ExodusException e) {
            disposeIfShouldBe();
            throw e;
        }
    }

    @Override
    @Nullable
    public Entity next() {
        throwNoSuchElementExceptionIfNecessary();
        checkDisposed();
        final EntityId nextEntityId = nextId();
        return nextEntityId == null ? null : iterable.getEntity(nextEntityId);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("EntityIterator.remove() is not supported.");
    }

    @Override
    public boolean skip(int number) {
        if (finished) {
            return false;
        }
        checkDisposed();
        try {
            while (number-- > 0 && hasNextImpl()) {
                nextIdImpl();
            }
            return hasNextImpl();
        } catch (ExodusException e) {
            disposeIfShouldBe();
            throw e;
        }
    }

    @Override
    @Nullable
    public EntityId nextId() {
        throwNoSuchElementExceptionIfNecessary();
        try {
            if ((++nextIdCounter & 0x1ff) == 0) {
                // do not check QueryCancellingPolicy too often
                final QueryCancellingPolicy cancellingPolicy = getQueryCancellingPolicy();
                if (cancellingPolicy != QueryCancellingPolicy.NONE && cancellingPolicy.needToCancel()) {
                    cancellingPolicy.doCancel();
                }
            }
            return nextIdImpl();
        } catch (ExodusException e) {
            disposeIfShouldBe();
            throw e;
        }
    }

    @Nullable
    public EntityId getLast() {
        EntityId result = null;
        while (hasNext()) {
            result = nextId();
        }
        return result;
    }

    @Override
    public boolean shouldBeDisposed() {
        final Cursor cursor = this.cursor;
        return cursor != null && cursor.isMutable();
    }

    @Override
    public boolean dispose() {
        queryCancellingPolicy = null;
        if (!disposed) {
            disposed = true;
            final PersistentStoreTransaction txn = iterable.getStore().getCurrentTransaction();
            // txn is null if dispose is called from PersistentStoreTransaction.close()
            if (txn != null) {
                txn.deregisterEntityIterator(this);
            }
            final Cursor cursor = this.cursor;
            if (cursor != null) {
                cursor.close();
                this.cursor = null;
            }
            return true;
        }
        return false;
    }

    public void disposeIfShouldBe() {
        if (shouldBeDisposed()) {
            dispose();
        }
    }

    @NotNull
    protected QueryCancellingPolicy getQueryCancellingPolicy() {
        QueryCancellingPolicy result = this.queryCancellingPolicy;
        if (result == null) {
            result = iterable.getTransaction().getQueryCancellingPolicy();
            if (result == null) {
                result = QueryCancellingPolicy.NONE;
            }
            queryCancellingPolicy = result;
        }
        return result;
    }

    protected PersistentEntityStoreImpl getStore() {
        return iterable.getStore();
    }

    protected Cursor getCursor() {
        return cursor;
    }

    protected void setCursor(@NotNull final Cursor cursor) {
        if (this.cursor != null) {
            throw new RuntimeException("EntityIterator: Cursor is already set.");
        }
        this.cursor = cursor;
    }

    protected int getIndex() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    protected EntityIdSet toSet() {
        return null;
    }

    private void throwNoSuchElementExceptionIfNecessary() {
        if (finished) {
            throw new NoSuchElementException();
        }
    }

    private void checkDisposed() {
        if (disposed) {
            throw new EntityStoreException("Can't access disposed EntityIterator.");
        }
    }

    protected abstract boolean hasNextImpl();

    @Nullable
    protected abstract EntityId nextIdImpl();
}
