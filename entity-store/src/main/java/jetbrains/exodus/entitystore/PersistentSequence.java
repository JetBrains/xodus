/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentSequence implements Sequence, FlushLog.Member {

    @NotNull
    private final Store store;
    @NotNull
    private final ArrayByteIterable idKeyEntry;
    private final String name;
    private final AtomicLong val;
    private final AtomicLong lastSavedValue;
    private boolean forcedUpdate = false;

    public PersistentSequence(@NotNull final PersistentStoreTransaction txn,
                              @NotNull final Store store,
                              @NotNull final String name) {
        this(txn, store, name, 0L);
    }

    public PersistentSequence(@NotNull final PersistentStoreTransaction txn,
                              @NotNull final Store store,
                              @NotNull final String name,
                              final long initialValue) {
        this.store = store;
        this.name = name;
        idKeyEntry = sequenceNameToEntry(name);
        long savedValue = loadValue(txn);
        if (savedValue == -1L) {
            savedValue = initialValue - 1;
        }
        val = new AtomicLong(savedValue);
        lastSavedValue = new AtomicLong(savedValue);
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "PersistentSequence " + '\'' + name + '\'' + ", value =" + val + ", last saved = " + lastSavedValue;
    }

    @Override
    public long get() {
        return val.get();
    }

    @Override
    public void set(final long l) {
        val.set(l);
    }

    public void forceSet(final long l) {
        forcedUpdate = true;
        val.set(l); // don't check old value, some database refactorings may use this
    }

    @Override
    public long increment() {
        return val.incrementAndGet();
    }

    @Override
    public void logOperations(final Transaction txn, final FlushLog flushLog) {
        final long value = val.get();
        if (forcedUpdate) {
            store.put(txn, idKeyEntry, LongBinding.longToCompressedEntry(value));
            flushLog.add(() -> {
                lastSavedValue.set(value);
                forcedUpdate = false;
            });
        } else if (value > lastSavedValue.get()) { // is dirty
            store.put(txn, idKeyEntry, LongBinding.longToCompressedEntry(value));
            flushLog.add(() -> {
                for (; ; ) {
                    final long current = lastSavedValue.get(); // never decrease
                    if (current >= value || lastSavedValue.compareAndSet(current, value)) {
                        break;
                    }
                }
            });
        }
    }

    void invalidate(@NotNull final Transaction txn) {
        set(loadValue(txn));
    }

    long loadValue(@NotNull final PersistentStoreTransaction txn) {
        return loadValue(txn.getEnvironmentTransaction());
    }

    private long loadValue(@NotNull final Transaction txn) {
        final ByteIterable value = store.get(txn, idKeyEntry);
        return value == null ? -1L : LongBinding.compressedEntryToLong(value);
    }

    static ArrayByteIterable sequenceNameToEntry(@NotNull final String sequenceName) {
        return new ArrayByteIterable(sequenceName.getBytes(StandardCharsets.UTF_8));
    }
}