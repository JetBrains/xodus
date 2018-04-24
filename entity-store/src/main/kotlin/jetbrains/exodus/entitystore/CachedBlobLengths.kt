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
package jetbrains.exodus.entitystore

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.IOUtil

class CachedBlobLengths(private val env: Environment,
                        private val blobFileLengths: Store) : VaultSizeFunctions {

    private val blockSize = IOUtil.getBlockSize()

    override fun getBlobVaultSize(): Long {
        return if (env.isOpen) {
            env.computeInReadonlyTransaction { txn ->
                var result: Long = 0
                blobFileLengths.openCursor(txn).use { cursor ->
                    while (cursor.next) {
                        result += adjustToBlockSize(cursor.value.toLength)
                    }
                }
                result
            }
        } else 0L
    }

    override fun getBlobSize(blobHandle: Long, txn: Transaction): Long {
        return if (env.isOpen) {
            blobFileLengths.get(txn, blobHandle.toEntry)?.toLength ?: 0L
        } else {
            0L
        }
    }

    private fun adjustToBlockSize(fileLength: Long): Long = (Math.max(fileLength, 1L) + blockSize - 1) / blockSize * blockSize

    companion object {

        private val ByteIterable.toLength: Long
            get() = LongBinding.compressedEntryToLong(this)
        private val Long.toEntry: ByteIterable
            get() = LongBinding.longToCompressedEntry(this)

    }
}
