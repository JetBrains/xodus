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
package jetbrains.exodus.tree

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor

interface ITreeCursor : Cursor {
    override fun isMutable(): Boolean
    val tree: ITree?

    companion object {
        @JvmField
        val EMPTY_CURSOR: ITreeCursor = object : ITreeCursor {
            override fun isMutable(): Boolean {
                return false
            }

            override fun getNext(): Boolean {
                return false
            }

            override fun getNextDup(): Boolean {
                return false
            }

            override fun getNextNoDup(): Boolean {
                return false
            }

            override fun getLast(): Boolean {
                return false
            }

            override fun getPrev(): Boolean {
                return false
            }

            override fun getPrevDup(): Boolean {
                return false
            }

            override fun getPrevNoDup(): Boolean {
                return false
            }

            override fun getKey(): ByteIterable {
                throw UnsupportedOperationException("No key found")
            }

            override fun getValue(): ByteIterable {
                throw UnsupportedOperationException("No value found")
            }

            override fun getSearchKey(key: ByteIterable): ByteIterable? {
                return null
            }

            override fun getSearchKeyRange(key: ByteIterable): ByteIterable? {
                return null
            }

            override fun getSearchBoth(key: ByteIterable, value: ByteIterable): Boolean {
                return false
            }

            override fun getSearchBothRange(key: ByteIterable, value: ByteIterable): ByteIterable? {
                return null
            }

            override fun count(): Int {
                return 0
            }

            override fun close() {}
            override val tree: ITree?
                get() = null

            override fun deleteCurrent(): Boolean {
                return false
            }
        }
    }
}
