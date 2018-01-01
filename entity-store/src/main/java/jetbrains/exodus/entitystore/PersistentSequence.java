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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentSequence implements Sequence, FlushLog.Member {

    @NonNls
    private static final String UTF8 = "UTF-8";

    @NotNull
    private final Store store;
    @NotNull
    private final ArrayByteIterable idKeyEntry;
    private final String name;
    private final AtomicLong val;
    private final AtomicLong lastSavedValue = new AtomicLong(-1);

    public PersistentSequence(@NotNull final PersistentStoreTransaction txn, @NotNull final Store store, @NotNull final String name) {
        this.store = store;
        this.name = name;
        idKeyEntry = sequenceNameToEntry(name);
        val = new AtomicLong(loadValue(txn));
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
        val.set(l); // don't check old value, some database refactorings may use this
    }

    @Override
    public long increment() {
        return val.incrementAndGet();
    }

    @Override
    public void logOperations(final Transaction txn, final FlushLog flushLog) {
        final long value = val.get();
        if (value > lastSavedValue.get()) { // is dirty
            store.put(txn, idKeyEntry, LongBinding.longToCompressedEntry(value));
            flushLog.add(new FlushLog.Operation() {
                @Override
                public void flushed() {
                    for (; ; ) {
                        final long current = lastSavedValue.get(); // never decrease
                        if (current >= value || lastSavedValue.compareAndSet(current, value)) {
                            break;
                        }
                    }
                }
            });
        }
    }

    long loadValue(@NotNull final PersistentStoreTransaction txn) {
        final ByteIterable value = store.get(txn.getEnvironmentTransaction(), idKeyEntry);
        return value == null ? -1 : LongBinding.compressedEntryToLong(value);
    }

    private static ArrayByteIterable sequenceNameToEntry(@NotNull final String sequenceName) {
        try {
            return new ArrayByteIterable(sequenceName.getBytes(UTF8));
        } catch (final UnsupportedEncodingException e) {
            throw new EntityStoreException(e);
        }
    }
}
