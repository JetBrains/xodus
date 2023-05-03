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
        for (snapshot in current) {
            executable.execute(snapshot.txn)
        }
    }

    fun add(txn: TransactionBase) {
        val snapshot = Snapshot(txn, txn.root)
        while (true) {
            val prevSet = snapshots.get()
            val newSet = prevSet.set.clone
            if (!newSet.contains(snapshot)) {
                val mutableSet = newSet.beginWrite()
                mutableSet.add(snapshot)
                mutableSet.endWrite()
            }
            val prevMin = prevSet.min
            val prevMax = prevSet.max
            val newMin = if (prevMin != null && prevMin.root > snapshot.root) snapshot else prevMin
            val newMax = if (prevMax != null && prevMax.root < snapshot.root) snapshot else prevMax
            if (snapshots.compareAndSet(prevSet, MinMaxAwareSnapshotSet(newSet, newMin, newMax))) {
                break
            }
        }
    }

    operator fun contains(txn: TransactionBase): Boolean {
        return current.contains(Snapshot(txn, 0))
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
            val prevMin = prevSet.min
            val prevMax = prevSet.max
            val newMin = if (prevMin == snapshot) null else prevMin
            val newMax = if (prevMax == snapshot) null else prevMax
            if (snapshots.compareAndSet(prevSet, MinMaxAwareSnapshotSet(newSet, newMin, newMax))) {
                break
            }
        }
    }

    val isEmpty: Boolean
        get() = current.isEmpty

    fun size(): Int {
        return current.size()
    }

    val oldestTxnRootAddress: Long
        get() {
            val oldestSnapshot = snapshots.get().min
            return oldestSnapshot?.root ?: Long.MAX_VALUE
        }
    val newestTxnRootAddress: Long
        get() {
            val newestSnapshot = snapshots.get().max
            return newestSnapshot?.root ?: Long.MIN_VALUE
        }
    private val current: PersistentHashSet<Snapshot>
        get() = snapshots.get().set

    private class MinMaxAwareSnapshotSet @JvmOverloads constructor(
        val set: PersistentHashSet<Snapshot> = PersistentHashSet(),
        @field:Volatile private var _min: Snapshot? = null, @field:Volatile private var _max: Snapshot? = null
    ) {

        val min: Snapshot?
            get() {
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

        val max: Snapshot?
            get() {
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

    private class Snapshot(val txn: Transaction, val root: Long) {
        override fun equals(other: Any?): Boolean {
            return this === other || other is Snapshot && txn == other.txn
        }

        override fun hashCode(): Int {
            return txn.hashCode()
        }
    }
}
