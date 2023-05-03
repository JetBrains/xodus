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
package jetbrains.exodus.tree.patricia

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getInt
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.SingleByteIterable.getIterable
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.ITreeCursor
import jetbrains.exodus.tree.ITreeMutable
import jetbrains.exodus.tree.LongIterator

open class PatriciaTreeWithDuplicates : PatriciaTreeDecorator {
    @JvmOverloads
    constructor(
        log: Log,
        rootAddress: Long,
        structureId: Int,
        empty: Boolean = false
    ) : super(if (empty) PatriciaTreeEmpty(log, structureId, false) else PatriciaTree(log, rootAddress, structureId))

    constructor(treeNoDuplicates: ITree) : super(treeNoDuplicates)

    override fun get(key: ByteIterable): ByteIterable? {
        treeNoDuplicates.openCursor().use { cursor ->
            val value = cursor.getSearchKeyRange(getEscapedKeyWithSeparator(key))
            if (value != null && value !== ByteIterable.EMPTY) {
                val keyLength = getInt(value)
                if (key.length == keyLength) {
                    val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                    if (key.compareTo(keyLength, noDupKey, keyLength) == 0) {
                        val offset = keyLength + 1
                        return noDupKey.subIterable(offset, noDupKey.length - offset)
                    }
                }
            }
            return null
        }
    }

    override fun hasPair(key: ByteIterable, value: ByteIterable): Boolean {
        return treeNoDuplicates.hasKey(getEscapedKeyValue(key, value))
    }

    override val mutableCopy: ITreeMutable
        get() = PatriciaTreeWithDuplicatesMutable(treeNoDuplicates.mutableCopy)

    override fun openCursor(): ITreeCursor {
        return PatriciaCursorDecorator(treeNoDuplicates.openCursor())
    }

    override fun addressIterator(): LongIterator {
        return treeNoDuplicates.addressIterator()
    }

    companion object {
        fun getEscapedKeyValue(key: ByteIterable, value: ByteIterable): ByteIterable {
            return CompoundByteIterable(
                arrayOf(
                    EscapingByteIterable(key),
                    getIterable(0.toByte()),
                    EscapingByteIterable(value)
                )
            )
        }

        fun getEscapedKeyWithSeparator(key: ByteIterable): ByteIterable {
            return CompoundByteIterable(
                arrayOf(
                    EscapingByteIterable(key),
                    getIterable(0.toByte())
                )
            )
        }
    }
}
