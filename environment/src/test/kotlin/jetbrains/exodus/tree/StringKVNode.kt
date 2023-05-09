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

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import java.io.PrintStream

/**
 */
class StringKVNode(key: String, value: String?) : INode {
    private val key: ByteIterable
    private val value: ByteIterable?
    private val skey: String
    private val svalue: String?

    override fun getKey(): ByteIterable = key

    override fun getValue(): ByteIterable? = value

    init {
        this.key = ArrayByteIterable(key.toByteArray())
        this.value = if (value == null) null else ArrayByteIterable(value.toByteArray())
        skey = key
        svalue = value
    }

    override fun equals(other: Any?): Boolean {
        return this === other || other is INode && key == other.getKey()
    }

    override fun hashCode(): Int {
        return key.hashCode()
    }

    override fun hasValue(): Boolean {
        return true
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        for (i in 1 until level) out.print(" ")
        out.print("*")
        out.println(if (renderer == null) toString() else renderer.toString(this))
    }

    override fun toString(): String {
        return "LN {key:$key ($skey), value:$value ($svalue)}"
    }
}
