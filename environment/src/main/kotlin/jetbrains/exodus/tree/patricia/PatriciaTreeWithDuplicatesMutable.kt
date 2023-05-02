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

import jetbrains.exodus.*
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getInt
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.tree.*
import jetbrains.exodus.tree.TreeCursorMutable.Companion.notifyCursors
import jetbrains.exodus.tree.patricia.PatriciaTreeMutable.Companion.getNotNullValue

internal class PatriciaTreeWithDuplicatesMutable(treeNoDuplicates: ITreeMutable) :
    PatriciaTreeWithDuplicates(treeNoDuplicates), ITreeMutable {
    override val mutableCopy: ITreeMutable
        get() = this

    override fun cursorClosed(cursor: ITreeCursorMutable) {
        throw UnsupportedOperationException()
    }

    override val root: MutableTreeRoot?
        get() = (this.treeNoDuplicates as ITreeMutable).root
    override val isAllowingDuplicates: Boolean
        get() = true
    override val openCursors: Iterable<ITreeCursorMutable>?
        get() = (this.treeNoDuplicates as ITreeMutable).openCursors

    override fun put(key: ByteIterable, value: ByteIterable): Boolean {
        return (this.treeNoDuplicates as ITreeMutable).put(
            getEscapedKeyValue(key, value), getIterable(key.length.toLong())
        )
    }

    override fun putRight(key: ByteIterable, value: ByteIterable) {
        (this.treeNoDuplicates as ITreeMutable).putRight(
            getEscapedKeyValue(key, value), getIterable(key.length.toLong())
        )
    }

    override fun add(key: ByteIterable, value: ByteIterable): Boolean {
        return (this.treeNoDuplicates as ITreeMutable).add(
            getEscapedKeyValue(key, value), getIterable(key.length.toLong())
        )
    }

    override fun put(ln: INode) {
        put(ln.key, getNotNullValue(ln))
    }

    override fun putRight(ln: INode) {
        putRight(ln.key, getNotNullValue(ln))
    }

    override fun add(ln: INode): Boolean {
        return add(ln.key, getNotNullValue(ln))
    }

    override fun delete(key: ByteIterable): Boolean {
        var wasDeleted = false
        treeNoDuplicates.openCursor().use { cursor ->
            val keyLength = key.length
            var value = cursor.getSearchKeyRange(getEscapedKeyWithSeparator(key))
            while (value != null) {
                if (keyLength != getInt(value)) {
                    break
                }
                val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                if (key.compareTo(keyLength, noDupKey, keyLength) != 0) {
                    break
                }
                cursor.deleteCurrent()
                wasDeleted = true
                value = if (cursor.next) cursor.value else null
            }
        }
        return wasDeleted
    }

    override fun delete(
        key: ByteIterable,
        value: ByteIterable?,
        cursorToSkip: ITreeCursorMutable?
    ): Boolean {
        if (value == null) {
            return delete(key)
        }
        if ((this.treeNoDuplicates as ITreeMutable).delete(getEscapedKeyValue(key, value))) {
            notifyCursors(this, cursorToSkip)
            return true
        }
        return false
    }

    override fun save(): Long {
        return (this.treeNoDuplicates as ITreeMutable).save()
    }

    override val expiredLoggables: ExpiredLoggableCollection
        get() = (this.treeNoDuplicates as ITreeMutable).expiredLoggables

    override fun reclaim(
        loggable: RandomAccessLoggable,
        loggables: Iterator<RandomAccessLoggable>
    ): Boolean {
        return (this.treeNoDuplicates as ITreeMutable).reclaim(loggable, loggables)
    }
}
