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
import java.io.PrintStream

interface INode : Dumpable {
    fun hasValue(): Boolean
    fun getKey(): ByteIterable
    fun getValue(): ByteIterable?

    companion object {
        val EMPTY: INode = object : INode {
            override fun hasValue(): Boolean {
                return false
            }

            override fun getKey(): ByteIterable = ByteIterable.EMPTY

            override fun getValue(): ByteIterable? = ByteIterable.EMPTY

            override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
                out.println("Empty node")
            }
        }
    }
}
