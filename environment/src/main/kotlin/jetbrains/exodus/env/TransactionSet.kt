/*
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
package jetbrains.exodus.env

import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet
import java.util.concurrent.atomic.AtomicReference

internal class TransactionSet {
    private val snapshots: AtomicReference<MinMaxAwareSnapshotSet> = AtomicReference(MinMaxAwareSnapshotSet())

    fun forEach(executable: TransactionalExecutable) {
        for (snapshot in getCurrent()) {
            executable.execute(snapshot.txn)
        }
    }

    fun add(txn: TransactionBase) {
        val snapshot = Snapshot(txn, txn.getRoot())
        while (true) {
            val prevSet = snapshots.get()
            val newSet = prevSet.set.clone
            if (!newSet.contains(snapshot)) {
                val mutableSet = newSet.beginWrite()
                mutableSet.add(snapshot)
                mutableSet.endWrite()
            }
            val prevMin = prevSet.getMin()
            val prevMax = prevSet.getMax()
            val newMin = if (prevMin != null && prevMin.root > snapshot.root) snapshot else prevMin
            val newMax = if (prevMax != null && prevMax.root < snapshot.root) snapshot else prevMax
            if (snapshots.compareAndSet(prevSet, MinMaxAwareSnapshotSet(newSet, newMin, newMax))) {
                break
            }
        }
    }

    operator fun contains(txn: TransactionBase): Boolean {
        return getCurrent().contains(Snapshot(txn, 0))
    }

    fun remove(txn: TransactionBase) {
        val snapshot = Snapshot(txn, 0)
        while (true) {
            val prevSet = snapshots.get()
            val newSet = prevSet.set.clone
            val mutableSet = newSet.beginWrite()
            if (!mutableSet.remove(snapshot)) {
                break
            }
            mutableSet.endWrite()
            // update min & max
            val prevMin = prevSet.getMin()
            val prevMax = prevSet.getMax()
            val newMin = if (prevMin == snapshot) null else prevMin
            val newMax = if (prevMax == snapshot) null else prevMax
            if (snapshots.compareAndSet(prevSet, MinMaxAwareSnapshotSet(newSet, newMin, newMax))) {
                break
            }
        }
    }

    fun isEmpty(): Boolean = getCurrent().isEmpty

    fun size(): Int {
        return getCurrent().size()
    }

    fun getOldestTxnRootAddress(): Long {
        val oldestSnapshot = snapshots.get().getMin()
        return oldestSnapshot?.root ?: Long.MAX_VALUE
    }

    fun getNewestTxnRootAddress(): Long {
        val newestSnapshot = snapshots.get().getMax()
        return newestSnapshot?.root ?: Long.MIN_VALUE
    }

    private fun getCurrent(): PersistentHashSet<Snapshot> = snapshots.get().set

    private class MinMaxAwareSnapshotSet(
        @JvmField val set: PersistentHashSet<Snapshot> = PersistentHashSet(),
        @Volatile private var _min: Snapshot? = null, @Volatile private var _max: Snapshot? = null
    ) {

        fun getMin(): Snapshot? {
            if (_min == null) {
                var min: Snapshot? = null
                for (snapshot in set) {
                    if (min == null || snapshot.root < min.root) {
                        min = snapshot
                    }
                }
                _min = min
            }
            return _min
        }

        fun getMax(): Snapshot? {
            if (_max == null) {
                var max: Snapshot? = null
                for (snapshot in set) {
                    if (max == null || snapshot.root > max.root) {
                        max = snapshot
                    }
                }
                _max = max
            }
            return _max
        }
    }

    private class Snapshot(@JvmField val txn: Transaction, @JvmField val root: Long) {
        override fun equals(other: Any?): Boolean {
            return this === other || other is Snapshot && txn == other.txn
        }

        override fun hashCode(): Int {
            return txn.hashCode()
        }
    }
}
