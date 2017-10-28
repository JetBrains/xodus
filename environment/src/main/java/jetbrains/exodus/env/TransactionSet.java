/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

final class TransactionSet {

    private final AtomicReference<MinMaxAwareSnapshotSet> snapshots;

    TransactionSet() {
        snapshots = new AtomicReference<>(new MinMaxAwareSnapshotSet());
    }

    void forEach(@NotNull final TransactionalExecutable executable) {
        for (final Snapshot snapshot : getCurrent()) {
            executable.execute(snapshot.txn);
        }
    }

    void add(@NotNull final TransactionBase txn) {
        final Snapshot snapshot = new Snapshot(txn, txn.getRoot());
        for (; ; ) {
            final MinMaxAwareSnapshotSet prevSet = snapshots.get();
            final PersistentHashSet<Snapshot> newSet = prevSet.set.getClone();
            if (!newSet.contains(snapshot)) {
                final PersistentHashSet.MutablePersistentHashSet<Snapshot> mutableSet = newSet.beginWrite();
                mutableSet.add(snapshot);
                mutableSet.endWrite();
            }
            final Snapshot prevMin = prevSet.min;
            final Snapshot prevMax = prevSet.max;
            final Snapshot newMin = prevMin != null && prevMin.root > snapshot.root ? snapshot : prevMin;
            final Snapshot newMax = prevMax != null && prevMax.root < snapshot.root ? snapshot : prevMax;
            if (this.snapshots.compareAndSet(prevSet, new MinMaxAwareSnapshotSet(newSet, newMin, newMax))) {
                break;
            }
        }
    }

    void remove(@NotNull final TransactionBase txn) {
        final Snapshot snapshot = new Snapshot(txn, 0);
        for (; ; ) {
            final MinMaxAwareSnapshotSet prevSet = snapshots.get();
            final PersistentHashSet<Snapshot> newSet = prevSet.set.getClone();
            final PersistentHashSet.MutablePersistentHashSet<Snapshot> mutableSet = newSet.beginWrite();
            if (!mutableSet.remove(snapshot)) {
                break;
            }
            mutableSet.endWrite();
            // update min & max
            final Snapshot prevMin = prevSet.min;
            final Snapshot prevMax = prevSet.max;
            final Snapshot newMin = Objects.equals(prevMin, snapshot) ? null : prevMin;
            final Snapshot newMax = Objects.equals(prevMax, snapshot) ? null : prevMax;
            if (this.snapshots.compareAndSet(prevSet, new MinMaxAwareSnapshotSet(newSet, newMin, newMax))) {
                break;
            }
        }
    }

    boolean isEmpty() {
        return getCurrent().isEmpty();
    }

    int size() {
        return getCurrent().size();
    }

    long getOldestTxnRootAddress() {
        final Snapshot oldestSnapshot = snapshots.get().getMin();
        return oldestSnapshot == null ? Long.MAX_VALUE : oldestSnapshot.root;
    }

    long getNewestTxnRootAddress() {
        final Snapshot newestSnapshot = snapshots.get().getMax();
        return newestSnapshot == null ? Long.MIN_VALUE : newestSnapshot.root;
    }

    @NotNull
    private PersistentHashSet<Snapshot> getCurrent() {
        return snapshots.get().set;
    }

    private static class MinMaxAwareSnapshotSet {

        @NotNull
        final PersistentHashSet<Snapshot> set;
        @Nullable
        volatile Snapshot min;
        @Nullable
        volatile Snapshot max;

        MinMaxAwareSnapshotSet(@NotNull final PersistentHashSet<Snapshot> set,
                               @Nullable final Snapshot min, @Nullable final Snapshot max) {
            this.set = set;
            this.min = min;
            this.max = max;
        }

        MinMaxAwareSnapshotSet() {
            this(new PersistentHashSet<Snapshot>(), null, null);
        }

        @Nullable
        Snapshot getMin() {
            if (min == null) {
                Snapshot min = null;
                for (final Snapshot snapshot : set) {
                    if (min == null || snapshot.root < min.root) {
                        min = snapshot;
                    }
                }
                this.min = min;
            }
            return min;
        }

        @Nullable
        Snapshot getMax() {
            if (max == null) {
                Snapshot max = null;
                for (final Snapshot snapshot : set) {
                    if (max == null || snapshot.root > max.root) {
                        max = snapshot;
                    }
                }
                this.max = max;
            }
            return max;
        }
    }

    private static class Snapshot {

        @NotNull
        final Transaction txn;
        final long root;

        Snapshot(@NotNull Transaction txn, long root) {
            this.txn = txn;
            this.root = root;
        }

        @Override
        public boolean equals(Object other) {
            return this == other || other instanceof Snapshot && txn.equals(((Snapshot)other).txn);
        }

        @Override
        public int hashCode() {
            return txn.hashCode();
        }
    }
}
