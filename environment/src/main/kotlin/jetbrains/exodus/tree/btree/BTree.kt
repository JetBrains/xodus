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
package jetbrains.exodus.tree.btree

import jetbrains.exodus.ExodusException
import jetbrains.exodus.log.*
import jetbrains.exodus.log.DataCorruptionException.Companion.raise

open class BTree(
    log: Log,
    policy: BTreeBalancePolicy,
    rootAddress: Long,
    allowsDuplicates: Boolean,
    structureId: Int
) : BTreeBase(log, policy, allowsDuplicates, structureId) {
    private val rootLoggable: RandomAccessLoggable?
    final override val root: BasePageImmutable

    constructor(log: Log, rootAddress: Long, allowsDuplicates: Boolean, structureId: Int) : this(
        log,
        BTreeBalancePolicy.DEFAULT,
        rootAddress,
        allowsDuplicates,
        structureId
    )

    init {
        require(rootAddress != Loggable.NULL_ADDRESS) { "Can't instantiate not empty tree with null root address." }
        rootLoggable = getLoggable(rootAddress)
        val type = rootLoggable.type
        // load size, but check if it exists
        if (type != BOTTOM_ROOT && type != INTERNAL_ROOT) {
            throw ExodusException("Unexpected root page type: $type")
        }
        val data = rootLoggable.data
        val it = data.iterator()
        size = it.compressedUnsignedLong
        root = loadRootPage(
            data.cloneWithAddressAndLength(it.address, it.available()),
            rootLoggable.isDataInsideSinglePage
        )
    }

    override val rootAddress: Long
        get() = rootLoggable!!.address
    override val mutableCopy: BTreeMutable
        get() {
            val result = BTreeMutable(this)
            result.addExpiredLoggable(rootLoggable!!)
            return result
        }

    private fun loadRootPage(
        data: ByteIterableWithAddress,
        loggableInsideSinglePage: Boolean
    ): BasePageImmutable {
        val result: BasePageImmutable
        val type = rootLoggable!!.type
        result = when (type) {
            LEAF_DUP_BOTTOM_ROOT, BOTTOM_ROOT, BOTTOM, DUP_BOTTOM -> BottomPage(
                this,
                data,
                loggableInsideSinglePage
            )

            LEAF_DUP_INTERNAL_ROOT, INTERNAL_ROOT, INTERNAL, DUP_INTERNAL -> InternalPage(
                this,
                data,
                loggableInsideSinglePage
            )

            else -> {
                raise("Unexpected loggable type: $type", log, rootLoggable.address)
                // dummy unreachable statement
                throw RuntimeException()
            }
        }
        return result
    }
}
