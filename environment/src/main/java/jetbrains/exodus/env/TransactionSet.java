/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

final class TransactionSet implements Iterable<TransactionImpl> {

    private final AtomicReference<MinMaxAwareTransactionSet> txns;

    TransactionSet() {
        txns = new AtomicReference<MinMaxAwareTransactionSet>(new MinMaxAwareTransactionSet());
    }

    @Override
    public Iterator<TransactionImpl> iterator() {
        return getCurrent().iterator();
    }

    void add(@NotNull final TransactionImpl txn) {
        final long root = txn.getRoot();
        for (; ; ) {
            final MinMaxAwareTransactionSet prevSet = txns.get();
            final PersistentHashSet<TransactionImpl> newSet = prevSet.set.getClone();
            final PersistentHashSet.MutablePersistentHashSet<TransactionImpl> mutableSet = newSet.beginWrite();
            final TransactionImpl prevMin = prevSet.min;
            final TransactionImpl newMin;
            if (mutableSet.contains(txn)) {
                newMin = prevMin == txn ? null : prevMin;
            } else {
                mutableSet.add(txn);
                mutableSet.endWrite();
                newMin = prevMin != null && prevMin.getRoot() > root ? txn : prevMin;
            }
            final TransactionImpl prevMax = prevSet.max;
            final TransactionImpl newMax = prevMax != null && prevMax.getRoot() < root ? txn : prevMax;
            if (this.txns.compareAndSet(prevSet, new MinMaxAwareTransactionSet(newSet, newMin, newMax))) {
                break;
            }
        }
    }

    boolean contains(@NotNull final TransactionImpl txn) {
        return getCurrent().contains(txn);
    }

    void remove(@NotNull final TransactionImpl txn) {
        for (; ; ) {
            final MinMaxAwareTransactionSet prevSet = txns.get();
            final PersistentHashSet<TransactionImpl> newSet = prevSet.set.getClone();
            final PersistentHashSet.MutablePersistentHashSet<TransactionImpl> mutableSet = newSet.beginWrite();
            if (!mutableSet.remove(txn)) {
                break;
            }
            mutableSet.endWrite();
            // update min & max
            final TransactionImpl prevMin = prevSet.min;
            final TransactionImpl newMin = prevMin == txn ? null : prevMin;
            final TransactionImpl prevMax = prevSet.max;
            final TransactionImpl newMax = prevMax == txn ? null : prevMax;
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
    TransactionImpl getOldestTransaction() {
        return txns.get().getMin();
    }

    @Nullable
    TransactionImpl getNewestTransaction() {
        return txns.get().getMax();
    }

    private PersistentHashSet.ImmutablePersistentHashSet<TransactionImpl> getCurrent() {
        return txns.get().set.getCurrent();
    }

    private static class MinMaxAwareTransactionSet {

        @NotNull
        private final PersistentHashSet<TransactionImpl> set;
        @Nullable
        private TransactionImpl min;
        @Nullable
        private TransactionImpl max;

        private MinMaxAwareTransactionSet(@NotNull final PersistentHashSet<TransactionImpl> set,
                                          @Nullable final TransactionImpl min, @Nullable final TransactionImpl max) {
            this.set = set;
            this.min = min;
            this.max = max;
        }

        private MinMaxAwareTransactionSet() {
            this(new PersistentHashSet<TransactionImpl>(), null, null);
        }

        @Nullable
        private TransactionImpl getMin() {
            if (min == null) {
                TransactionImpl min = null;
                long minRoot = Long.MIN_VALUE;
                for (final TransactionImpl txn : set.getCurrent()) {
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
        private TransactionImpl getMax() {
            if (max == null) {
                TransactionImpl max = null;
                long maxRoot = Long.MAX_VALUE;
                for (final TransactionImpl txn : set.getCurrent()) {
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
