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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.entitystore.tables.TwoColumnTable;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.StringInterner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class PersistentSequentialDictionary implements FlushLog.Member {

    @NotNull
    private final PersistentSequence sequence;
    @NotNull
    private final TwoColumnTable table;
    @NotNull
    private final Map<String, Integer> cache = new ConcurrentHashMap<>();
    @NotNull
    private final Map<Integer, String> reverseCache = new ConcurrentHashMap<>();
    @NotNull
    private final Collection<DictionaryOperation> operationsLog = new HashSet<>();
    private final Object lock = new Object();

    public PersistentSequentialDictionary(@NotNull PersistentSequence sequence, @NotNull TwoColumnTable table) {
        this.sequence = sequence;
        this.table = table;
    }

    @NotNull
    public TwoColumnTable getTable() { // synchronized modifier not needed
        return table;
    }

    public int getLastAllocatedId() { // synchronized modifier not needed
        return (int) sequence.get();
    }

    public int getId(@NotNull final PersistentStoreTransaction txn, @NotNull final String name) {
        return getId((TxnProvider) txn, name);
    }

    public int getId(@NotNull final TxnProvider txnProvider, @NotNull final String name) {
        Integer result = cache.get(name);
        if (result != null) {
            return result;
        }
        synchronized (lock) {
            result = cache.get(name);
            if (result != null) {
                return result;
            }
            final ByteIterable idEntry = table.get(txnProvider.getTransaction().getEnvironmentTransaction(), StringBinding.stringToEntry(name));
            if (idEntry != null) {
                final int id = IntegerBinding.compressedEntryToInt(idEntry);
                putIdUnsafe(name, id);
                return id;
            }
            putNoIdUnsafe(name);
        }
        return -1;
    }

    public int getOrAllocateId(@NotNull final PersistentStoreTransaction txn, @NotNull final String name) {
        return getOrAllocateId((TxnProvider) txn, name);
    }

    public int getOrAllocateId(@NotNull final TxnProvider txnProvider, @NotNull final String name) {
        Integer result = cache.get(name);
        if (result != null && result >= 0) {
            return result;
        }
        synchronized (lock) {
            result = cache.get(name);
            if (result != null && result >= 0) {
                return result;
            }
            final ByteIterable nameEntry = StringBinding.stringToEntry(name);
            final PersistentStoreTransaction txn = txnProvider.getTransaction();
            final ByteIterable idEntry = table.get(txn.getEnvironmentTransaction(), nameEntry);
            final int id = idEntry == null ? (int) sequence.increment() : IntegerBinding.compressedEntryToInt(idEntry);
            putIdUnsafe(name, id);
            if (idEntry == null) {
                operationsLog.add(new DictionaryOperation() {
                    @Override
                    public void persist(final Transaction txn) {
                        table.put(txn, nameEntry, IntegerBinding.intToCompressedEntry(id));
                    }
                });
                created(txn, id);
            }
            return id;
        }
    }

    @Nullable
    public String getName(@NotNull final PersistentStoreTransaction txn, final int id) {
        return getName((TxnProvider) txn, id);
    }

    @Nullable
    public String getName(@NotNull final TxnProvider txnProvider, final int id) {
        String result = reverseCache.get(id);
        if (result == null) {
            synchronized (lock) {
                final ByteIterable idEntry = IntegerBinding.intToCompressedEntry(id);
                final ByteIterable typeEntry = table.get2(txnProvider.getTransaction().getEnvironmentTransaction(), idEntry);
                if (typeEntry != null) {
                    result = StringBinding.entryToString(typeEntry);
                    if (result != null) {
                        reverseCache.put(id, result);
                    }
                }
            }
        }
        return result;
    }

    public int delete(@NotNull final PersistentStoreTransaction txn, @NotNull final String name) {
        final int id = getId(txn, name);
        if (id < 0) {
            // type doesn't exist, and it's ok
            return -1;
        }
        synchronized (lock) {
            operationsLog.add(new DictionaryOperation() {
                @Override
                void persist(final Transaction txn) {
                    table.delete(txn, StringBinding.stringToEntry(name), IntegerBinding.intToCompressedEntry(id));
                }
            });
            cache.remove(name);
            reverseCache.remove(id);
            return id;
        }
    }

    public void rename(@NotNull final PersistentStoreTransaction txn, @NotNull final String oldName, @NotNull final String newName) {
        if (oldName.equals(newName)) {
            return;
        }
        final int id = getId(txn, oldName);
        if (id < 0) {
            throw new IllegalArgumentException("Old entity type doesn't exist: " + oldName);
        }
        final int newId = getId(txn, newName);
        final ByteIterable idEntry = IntegerBinding.intToCompressedEntry(id);
        synchronized (lock) {
            operationsLog.add(new DictionaryOperation() {
                @Override
                void persist(final Transaction txn) {
                    table.delete(txn, StringBinding.stringToEntry(oldName), idEntry);
                    table.put(txn, StringBinding.stringToEntry(newName), idEntry);
                }
            });
            cache.remove(oldName);
            cache.put(newName, id);
            reverseCache.remove(id);
            if (newId >= 0) {
                reverseCache.remove(newId);
            }
        }
    }

    protected void created(final PersistentStoreTransaction txn, final int id) { // synchronized modifier not needed
    }

    @Override
    public void logOperations(final Transaction txn, final FlushLog flushLog) {
        synchronized (lock) {
            for (final DictionaryOperation op : operationsLog) {
                op.persist(txn);
                flushLog.add(op);
            }
        }
    }

    private void putIdUnsafe(@NotNull final String name, final int id) {
        final String nameInterned = StringInterner.intern(name);
        cache.put(nameInterned, id);
        reverseCache.put(id, nameInterned);
    }

    private void putNoIdUnsafe(@NotNull final String name) {
        cache.put(StringInterner.intern(name), -1);
    }

    private abstract class DictionaryOperation implements FlushLog.Operation {

        abstract void persist(final Transaction txn);

        @Override
        public void flushed() {
            synchronized (lock) {
                operationsLog.remove(this);
            }
        }
    }
}