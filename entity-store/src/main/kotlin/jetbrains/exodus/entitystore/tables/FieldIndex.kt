/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.tables

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.env.*

/**
 * Field index saves pairs (fieldId, localId) where fieldId is an id of property, link or blob,
 * and localId is a local id of the entity having a value of property, link or blob.
 * Use [iterable], to get all entities (their localIds) having specified property, link or blob.
 */
sealed class FieldIndex {

    abstract fun getStore(): Store

    abstract fun put(envTxn: Transaction, fieldId: Int, localId: Long): Boolean

    abstract fun remove(envTxn: Transaction, fieldId: Int, localId: Long): Boolean

    abstract fun iterable(envTxn: Transaction, fieldId: Int): Iterable<Pair<Int, Long>>

    companion object {

        @JvmStatic
        fun fieldIndex(txn: PersistentStoreTransaction, name: String): FieldIndex {
            val store = txn.store
            return if (!store.environment.environmentConfig.useVersion1Format && store.config.useIntForLocalId) {
                BitmapFieldIndex(txn, name)
            } else {
                StoreFieldIndex(txn, name)
            }.also { store.trackTableCreation(it.getStore(), txn) }
        }
    }
}

private class StoreFieldIndex(txn: PersistentStoreTransaction, name: String) : FieldIndex() {

    private val theIndex: Store

    init {
        txn.environmentTransaction.let { envTxn ->
            theIndex = envTxn.environment.openStore(
                name + Table.ALL_IDX,
                StoreConfig.WITH_DUPLICATES_WITH_PREFIXING,
                envTxn
            )
        }
    }

    override fun getStore(): Store = theIndex

    override fun put(envTxn: Transaction, fieldId: Int, localId: Long): Boolean =
        theIndex.put(
            envTxn,
            IntegerBinding.intToCompressedEntry(fieldId),
            LongBinding.longToCompressedEntry(localId)
        )

    override fun remove(envTxn: Transaction, fieldId: Int, localId: Long): Boolean {
        var result = true
        val key = IntegerBinding.intToCompressedEntry(fieldId)
        val value = LongBinding.longToCompressedEntry(localId)
        theIndex.openCursor(envTxn).let { cursor ->
            if (!cursor.getSearchBoth(key, value)) {
                // repeat for debugging
                cursor.getSearchBoth(key, value)
                result = false
            }
            if (!cursor.deleteCurrent()) {
                // repeat for debugging
                cursor.deleteCurrent()
                result = false
            }
        }
        return result
    }

    override fun iterable(envTxn: Transaction, fieldId: Int) =
        Iterable { StoreFieldIndexIterator(envTxn, theIndex, fieldId) }
}

private class BitmapFieldIndex(txn: PersistentStoreTransaction, name: String) : FieldIndex() {

    private val theIndex: BitmapImpl

    init {
        txn.environmentTransaction.let { envTxn ->
            theIndex = envTxn.environment.openBitmap(
                name + Table.ALL_IDX,
                StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING,
                envTxn
            ) as BitmapImpl
        }
    }

    override fun getStore(): Store = theIndex.store

    override fun put(envTxn: Transaction, fieldId: Int, localId: Long): Boolean =
        theIndex.set(envTxn, toBitIndex(fieldId, localId), true)

    override fun remove(envTxn: Transaction, fieldId: Int, localId: Long): Boolean =
        theIndex.clear(envTxn, toBitIndex(fieldId, localId))

    override fun iterable(envTxn: Transaction, fieldId: Int) =
        Iterable {
            object : Iterator<Pair<Int, Long>> {
                private val bitmapIterator: BitmapIterator = theIndex.iterator(envTxn)

                init {
                    bitmapIterator.getSearchBit(fieldId.toLong() shl 32)
                }

                override fun hasNext(): Boolean = bitmapIterator.hasNext()

                override fun next(): Pair<Int, Long> =
                    bitmapIterator.next().let { nextBit ->
                        val propId = (nextBit ushr 32).toInt()
                        val localId = nextBit and 0xffffffff
                        return Pair(propId, localId)
                    }
            }
        }

    private fun toBitIndex(propertyId: Int, localId: Long) = (propertyId.toLong() shl 32) + localId
}

private class StoreFieldIndexIterator(
    val txn: Transaction,
    val store: Store,
    val propertyId: Int
) : Iterator<Pair<Int, Long>> {

    private val cursor: Cursor = store.openCursor(txn)
    private var next: Pair<Int, Long>? = null
    private var hasNext: Boolean = false

    init {
        hasNext = cursor.getSearchKeyRange(IntegerBinding.intToCompressedEntry(propertyId)) != null
        setNext()
    }

    override fun hasNext(): Boolean = next != null

    override fun next(): Pair<Int, Long> {
        next?.also {
            setNext()
            return it
        }
        throw NoSuchElementException()
    }

    private fun setNext() {
        next = null
        if (hasNext) {
            next =
                Pair(IntegerBinding.compressedEntryToInt(cursor.key), LongBinding.compressedEntryToLong(cursor.value))
            hasNext = cursor.next
        }
    }
}