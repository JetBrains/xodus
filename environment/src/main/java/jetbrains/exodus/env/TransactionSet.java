/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

final class TransactionSet implements Iterable<TransactionBase> {

    private final AtomicReference<MinMaxAwareTransactionSet> txns;

    TransactionSet() {
        txns = new AtomicReference<>(new MinMaxAwareTransactionSet());
    }

    @Override
    public Iterator<TransactionBase> iterator() {
        return getCurrent().iterator();
    }

    void add(@NotNull final TransactionBase txn) {
        final long root = txn.getRoot();
        for (; ; ) {
            final MinMaxAwareTransactionSet prevSet = txns.get();
            final PersistentHashSet<TransactionBase> newSet = prevSet.set.getClone();
            final PersistentHashSet.MutablePersistentHashSet<TransactionBase> mutableSet = newSet.beginWrite();
            final TransactionBase prevMin = prevSet.min;
            final TransactionBase newMin;
            if (mutableSet.contains(txn)) {
                newMin = prevMin == txn ? null : prevMin;
            } else {
                mutableSet.add(txn);
                mutableSet.endWrite();
                newMin = prevMin != null && prevMin.getRoot() > root ? txn : prevMin;
            }
            final TransactionBase prevMax = prevSet.max;
            final TransactionBase newMax = prevMax != null && prevMax.getRoot() < root ? txn : prevMax;
            if (this.txns.compareAndSet(prevSet, new MinMaxAwareTransactionSet(newSet, newMin, newMax))) {
                break;
            }
        }
    }

    boolean contains(@NotNull final TransactionBase txn) {
        return getCurrent().contains(txn);
    }

    void remove(@NotNull final TransactionBase txn) {
        for (; ; ) {
            final MinMaxAwareTransactionSet prevSet = txns.get();
            final PersistentHashSet<TransactionBase> newSet = prevSet.set.getClone();
            final PersistentHashSet.MutablePersistentHashSet<TransactionBase> mutableSet = newSet.beginWrite();
            if (!mutableSet.remove(txn)) {
                break;
            }
            mutableSet.endWrite();
            // update min & max
            final TransactionBase prevMin = prevSet.min;
            final TransactionBase newMin = prevMin == txn ? null : prevMin;
            final TransactionBase prevMax = prevSet.max;
            final TransactionBase newMax = prevMax == txn ? null : prevMax;
            if (this.txns.compareAndSet(prevSet, new MinMaxAwareTransactionSet(newSet, newMin, newMax))) {
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

    @Nullable
    TransactionBase getOldestTransaction() {
        return txns.get().getMin();
    }

    @Nullable
    TransactionBase getNewestTransaction() {
        return txns.get().getMax();
    }

    private PersistentHashSet.ImmutablePersistentHashSet<TransactionBase> getCurrent() {
        return txns.get().set.getCurrent();
    }

    private static class MinMaxAwareTransactionSet {

        @NotNull
        private final PersistentHashSet<TransactionBase> set;
        @Nullable
        private TransactionBase min;
        @Nullable
        private TransactionBase max;

        private MinMaxAwareTransactionSet(@NotNull final PersistentHashSet<TransactionBase> set,
                                          @Nullable final TransactionBase min, @Nullable final TransactionBase max) {
            this.set = set;
            this.min = min;
            this.max = max;
        }

        private MinMaxAwareTransactionSet() {
            this(new PersistentHashSet<TransactionBase>(), null, null);
        }

        @Nullable
        private TransactionBase getMin() {
            if (min == null) {
                TransactionBase min = null;
                long minRoot = Long.MIN_VALUE;
                for (final TransactionBase txn : set.getCurrent()) {
                    final long root = txn.getRoot();
                    if (min == null || root < minRoot) {
                        min = txn;
                        minRoot = root;
                    }
                }
                this.min = min;
            }
            return min;
        }

        @Nullable
        private TransactionBase getMax() {
            if (max == null) {
                TransactionBase max = null;
                long maxRoot = Long.MAX_VALUE;
                for (final TransactionBase txn : set.getCurrent()) {
                    final long root = txn.getRoot();
                    if (max == null || root > maxRoot) {
                        max = txn;
                        maxRoot = root;
                    }
                }
                this.max = max;
            }
            return max;
        }
    }
}
