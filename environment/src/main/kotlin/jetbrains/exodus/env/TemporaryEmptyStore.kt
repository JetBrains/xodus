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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.tree.ITreeCursor
import jetbrains.exodus.tree.TreeMetaInfo

internal class TemporaryEmptyStore @JvmOverloads constructor(
    env: EnvironmentImpl,
    name: String = "Temporary Empty Store"
) : StoreImpl(env, name, TreeMetaInfo.EMPTY.clone(-1)) {
    override fun getConfig(): StoreConfig {
        return StoreConfig.TEMPORARY_EMPTY
    }

    override fun get(txn: Transaction, key: ByteIterable): ByteIterable? {
        return null
    }

    override fun exists(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ): Boolean {
        return false
    }

    override fun put(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ): Boolean {
        return throwCantModify(txn)
    }

    override fun putRight(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ) {
        throwCantModify(txn)
    }

    override fun add(
        txn: Transaction,
        key: ByteIterable,
        value: ByteIterable
    ): Boolean {
        return throwCantModify(txn)
    }

    override fun delete(txn: Transaction, key: ByteIterable): Boolean {
        return throwCantModify(txn)
    }

    override fun count(txn: Transaction): Long {
        return 0
    }

    override fun openCursor(txn: Transaction): Cursor {
        return ITreeCursor.EMPTY_CURSOR
    }

    override fun reclaim(
        transaction: Transaction,
        loggable: RandomAccessLoggable,
        loggables: Iterator<RandomAccessLoggable>
    ) {
        // nothing to reclaim
    }

    private fun throwCantModify(txn: Transaction): Boolean {
        if (txn.isReadonly) {
            throw ReadonlyTransactionException()
        }
        throw UnsupportedOperationException("Can't modify temporary empty store")
    }
}
