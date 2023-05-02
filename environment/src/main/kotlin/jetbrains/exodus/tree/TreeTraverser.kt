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

interface TreeTraverser {
    fun init(left: Boolean)
    val isNotEmpty: Boolean
    val key: ByteIterable
    val value: ByteIterable
    fun hasValue(): Boolean
    fun canMoveRight(): Boolean
    fun canMoveLeft(): Boolean
    fun canMoveUp(): Boolean
    fun canMoveDown(): Boolean
    fun moveRight(): INode
    fun moveLeft(): INode
    fun moveUp()
    fun moveDown(): INode
    fun moveDownToLast(): INode
    fun compareCurrent(key: ByteIterable): Int
    fun reset(root: MutableTreeRoot)
    val currentAddress: Long
    val tree: ITree?
    fun moveTo(key: ByteIterable, value: ByteIterable?): Boolean
    fun moveToRange(key: ByteIterable, value: ByteIterable?): Boolean
}