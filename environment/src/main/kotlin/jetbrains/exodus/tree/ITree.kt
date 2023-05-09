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
import jetbrains.exodus.log.DataIterator
import jetbrains.exodus.log.Log
import java.io.PrintStream

/**
 * Interface for immutable tree implementations
 */
interface ITree {
    fun getLog(): Log
    fun getDataIterator(address: Long): DataIterator
    fun getRootAddress(): Long
    fun getStructureId(): Int
    operator fun get(key: ByteIterable): ByteIterable?
    fun hasPair(key: ByteIterable, value: ByteIterable): Boolean
    fun hasKey(key: ByteIterable): Boolean
    fun getMutableCopy(): ITreeMutable
    fun isEmpty(): Boolean
    fun size(): Long
    fun openCursor(): ITreeCursor
    fun addressIterator(): LongIterator
    fun dump(out: PrintStream)
    fun dump(out: PrintStream, renderer: Dumpable.ToString?)
}
