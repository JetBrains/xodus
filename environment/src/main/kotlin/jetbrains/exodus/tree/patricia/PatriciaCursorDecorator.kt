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
import jetbrains.exodus.tree.ITree
import jetbrains.exodus.tree.ITreeCursor

class PatriciaCursorDecorator(private var patriciaCursor: ITreeCursor) : ITreeCursor {
    private var keyBytes: ByteIterable? = null
    private var keyLength = 0
    private var valueLength = 0
    private var nextKeyBytes: ByteIterable? = null
    private var nextKeyLength = UNKNOWN
    private var nextValueLength = 0
    private var prevKeyBytes: ByteIterable? = null
    private var prevKeyLength = UNKNOWN
    private var prevValueLength = 0
    override fun isMutable(): Boolean {
        return patriciaCursor.isMutable
    }

    override fun getTree(): ITree? = patriciaCursor.getTree()

    override fun getNext(): Boolean {
        if (getNextLazy()) {
            advance()
            return true
        }
        return false
    }

    override fun getNextDup(): Boolean {
        checkNotNull(keyBytes) { "Cursor is not yet initialized" }
        if (getNextLazy() && keyBytes!!.compareTo(keyLength, nextKeyBytes, nextKeyLength) == 0) {
            advance()
            return true
        }
        return false
    }

    override fun getNextNoDup(): Boolean {
        if (keyBytes == null) {
            return next // init
        }
        if (getNextLazy() && keyBytes!!.compareTo(keyLength, nextKeyBytes, nextKeyLength) != 0) {
            advance()
            return true
        }
        // we must create new cursor 'cause we don't know if next "no dup" pair exists
        val cursor = patriciaCursor.getTree()!!.openCursor()
        var cursorToClose = cursor
        try {
            if (cursor.getSearchKeyRange(PatriciaTreeWithDuplicates.getEscapedKeyValue(key, value)) != null) {
                while (cursor.next) {
                    val keyLengthIterable = cursor.value
                    val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                    val keyLength = getInt(keyLengthIterable)
                    if (keyBytes!!.compareTo(this.keyLength, noDupKey, keyLength) != 0) {
                        keyBytes = noDupKey
                        this.keyLength = keyLength
                        valueLength = noDupKey.length - keyLength - 1
                        cursorToClose = patriciaCursor
                        patriciaCursor = cursor
                        nextKeyLength = UNKNOWN // forget computed next pair
                        prevKeyLength = UNKNOWN // forget computed prev pair
                        return true
                    }
                }
            }
        } finally {
            cursorToClose.close()
        }
        return false
    }

    override fun getPrev(): Boolean {
        if (getPrevLazy()) {
            retreat()
            return true
        }
        return false
    }

    override fun getPrevDup(): Boolean {
        checkNotNull(keyBytes) { "Cursor is not yet initialized" }
        if (getPrevLazy() && keyBytes!!.compareTo(keyLength, prevKeyBytes, prevKeyLength) == 0) {
            retreat()
            return true
        }
        return false
    }

    override fun getPrevNoDup(): Boolean {
        if (keyBytes == null) {
            return prev // init
        }
        if (getPrevLazy() && keyBytes!!.compareTo(keyLength, prevKeyBytes, prevKeyLength) != 0) {
            advance()
            return true
        }
        // we must create new cursor 'cause we don't know if prev "no dup" pair exists
        val cursor = patriciaCursor.getTree()!!.openCursor()
        var cursorToClose = cursor
        try {
            if (cursor.getSearchKeyRange(PatriciaTreeWithDuplicates.getEscapedKeyValue(key, value)) != null) {
                while (cursor.prev) {
                    val keyLengthIterable = cursor.value
                    val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                    val keyLength = getInt(keyLengthIterable)
                    if (keyBytes!!.compareTo(this.keyLength, noDupKey, keyLength) != 0) {
                        keyBytes = noDupKey
                        this.keyLength = keyLength
                        valueLength = noDupKey.length - keyLength - 1
                        cursorToClose = patriciaCursor
                        patriciaCursor = cursor
                        prevKeyLength = UNKNOWN // forget computed prev pair
                        nextKeyLength = UNKNOWN // forget computed next pair
                        return true
                    }
                }
            }
        } finally {
            cursorToClose.close()
        }
        return false
    }

    override fun getLast(): Boolean {
        if (patriciaCursor.last) {
            val keyLengthIterable = patriciaCursor.value
            val sourceKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(patriciaCursor.key))
            keyBytes = sourceKey
            keyLength = getInt(keyLengthIterable)
            valueLength = sourceKey.length - keyLength - 1
            nextKeyLength = UNKNOWN // forget computed next pair
            prevKeyLength = UNKNOWN // forget computed prev pair
            return true
        }
        return false
    }

    override fun getKey(): ByteIterable {
        return if (keyBytes == null) ByteIterable.EMPTY else keyBytes!!.subIterable(0, keyLength)
    }

    override fun getValue(): ByteIterable {
        if (keyBytes == null) {
            return ByteIterable.EMPTY
        }
        val offset = keyLength + 1
        return keyBytes!!.subIterable(offset, valueLength)
    }

    override fun getSearchKey(key: ByteIterable): ByteIterable? {
        val cursor = patriciaCursor.getTree()!!.openCursor()
        var cursorToClose = cursor
        try {
            val keyLengthIterable = cursor.getSearchKeyRange(EscapingByteIterable(key))
            if (keyLengthIterable != null) {
                val keyLength = getInt(keyLengthIterable)
                if (key.length == keyLength) {
                    val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                    if (key.compareTo(keyLength, noDupKey, keyLength) == 0) {
                        keyBytes = noDupKey
                        this.keyLength = keyLength
                        valueLength = noDupKey.length - keyLength - 1
                        cursorToClose = patriciaCursor
                        patriciaCursor = cursor
                        nextKeyLength = UNKNOWN // forget computed next pair
                        prevKeyLength = UNKNOWN // forget computed prev pair
                        return value
                    }
                }
            }
        } finally {
            cursorToClose.close()
        }
        return null
    }

    override fun getSearchKeyRange(key: ByteIterable): ByteIterable? {
        val keyLengthIterable = patriciaCursor.getSearchKeyRange(EscapingByteIterable(key)) ?: return null
        val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(patriciaCursor.key))
        keyBytes = noDupKey
        keyLength = getInt(keyLengthIterable)
        valueLength = noDupKey.length - keyLength - 1
        nextKeyLength = UNKNOWN // forget computed next pair
        prevKeyLength = UNKNOWN // forget computed prev pair
        return value
    }

    override fun getSearchBoth(key: ByteIterable, value: ByteIterable): Boolean {
        val cursor = patriciaCursor.getTree()!!.openCursor()
        var cursorToClose = cursor
        return try {
            val keyLengthIterable =
                cursor.getSearchKey(PatriciaTreeWithDuplicates.getEscapedKeyValue(key, value))
                    ?: return false
            val keyLength = getInt(keyLengthIterable)
            if (keyLength == key.length) {
                keyBytes = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                this.keyLength = keyLength
                valueLength = value.length
                cursorToClose = patriciaCursor
                patriciaCursor = cursor
                nextKeyLength = UNKNOWN // forget computed next pair
                prevKeyLength = UNKNOWN // forget computed prev pair
                return true
            }
            false
        } finally {
            cursorToClose.close()
        }
    }

    override fun getSearchBothRange(key: ByteIterable, value: ByteIterable): ByteIterable? {
        val cursor = patriciaCursor.getTree()!!.openCursor()
        var cursorToClose = cursor
        try {
            var keyLengthIterable = cursor.getSearchKeyRange(EscapingByteIterable(key))
            if (keyLengthIterable != null) {
                val srcKeyLength = key.length
                val valueLength = value.length
                while (true) {
                    val keyLength = getInt(keyLengthIterable!!)
                    if (srcKeyLength == keyLength) {
                        val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                        if (key.compareTo(keyLength, noDupKey, keyLength) == 0) {
                            // skip separator
                            val noDupKeyLength = noDupKey.length - keyLength - 1
                            val cmp = noDupKey.compareTo(
                                keyLength + 1,
                                keyLength + 1 + noDupKeyLength, value, 0, valueLength
                            )
                            if (cmp >= 0) {
                                keyBytes = noDupKey
                                this.keyLength = keyLength
                                this.valueLength = noDupKeyLength
                                cursorToClose = patriciaCursor
                                patriciaCursor = cursor
                                nextKeyLength = UNKNOWN // forget computed next pair
                                prevKeyLength = UNKNOWN // forget computed prev pair
                                return getValue()
                            }
                        }
                    }
                    keyLengthIterable = if (cursor.next) {
                        cursor.value
                    } else {
                        break
                    }
                }
            }
        } finally {
            cursorToClose.close()
        }
        return null
    }

    override fun count(): Int {
        var result = 0
        patriciaCursor.getTree()!!.openCursor().use { cursor ->
            var value = cursor.getSearchKeyRange(EscapingByteIterable(key))
            while (value != null) {
                if (keyLength != getInt(value)) {
                    break
                }
                val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(cursor.key))
                if (keyBytes!!.compareTo(keyLength, noDupKey, keyLength) != 0) {
                    break
                }
                ++result
                value = if (cursor.next) cursor.value else null
            }
        }
        return result
    }

    override fun close() {
        patriciaCursor.close()
    }

    override fun deleteCurrent(): Boolean {
        return patriciaCursor.deleteCurrent()
    }

    private fun advance() {
        prevKeyBytes = keyBytes
        prevKeyLength = keyLength
        prevValueLength = valueLength
        keyBytes = nextKeyBytes
        keyLength = nextKeyLength
        valueLength = nextValueLength
        nextKeyLength = UNKNOWN // forget computed next pair
    }

    private fun getNextLazy(): Boolean {
        if (nextKeyLength < 0) { // UNKNOWN
            if (patriciaCursor.next) {
                val keyLengthIterable = patriciaCursor.value
                val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(patriciaCursor.key))
                nextKeyBytes = noDupKey
                nextKeyLength = getInt(keyLengthIterable)
                nextValueLength = noDupKey.length - nextKeyLength - 1
                return true
            }
            return false
        }
        return nextKeyBytes != null
    }

    private fun retreat() {
        nextKeyBytes = keyBytes
        nextKeyLength = keyLength
        nextValueLength = valueLength
        keyBytes = prevKeyBytes
        keyLength = prevKeyLength
        valueLength = prevValueLength
        prevKeyLength = UNKNOWN // forget computed prev pair
    }

    private fun getPrevLazy(): Boolean {
        if (prevKeyLength < 0) { // UNKNOWN
            if (patriciaCursor.prev) {
                val keyLengthIterable = patriciaCursor.value
                val noDupKey: ByteIterable = ArrayByteIterable(UnEscapingByteIterable(patriciaCursor.key))
                prevKeyBytes = noDupKey
                prevKeyLength = getInt(keyLengthIterable)
                prevValueLength = noDupKey.length - prevKeyLength - 1
                return true
            }
            return false
        }
        return prevKeyBytes != null
    }

    companion object {
        private const val UNKNOWN = -1
    }
}
