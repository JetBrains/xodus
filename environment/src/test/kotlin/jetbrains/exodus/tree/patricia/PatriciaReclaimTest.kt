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


import jetbrains.exodus.log.RandomAccessLoggable
import org.junit.Test

class PatriciaReclaimTest : PatriciaTestBase() {
    @Test
    fun testSimple() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv("aab", "aab"))
        treeMutable!!.put(kv("aabc", "aabc"))
        treeMutable!!.put(kv("aac", "aac"))
        treeMutable!!.put(kv("aacd", "aacd"))
        val a = saveTree()
        tree = openTree(a, false)
        treeMutable = tree!!.mutableCopy as PatriciaTreeMutable
        val loggables: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(0)
        treeMutable!!.reclaim(loggables.next(), loggables)
        assertMatches(
            treeMutable!!,
            rm(
                "aa",
                nm('b', "", "aab", nm('c', "", "aabc")),
                nm('c', "", "aac", nm('d', "", "aacd"))
            )
        )
    }

    @Test
    fun testSplitAndReplace() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv("aaa", "0"))
        treeMutable!!.put(kv("abbaa", "1"))
        treeMutable!!.put(kv("aca", "3")) // should be reclaimed
        var a = saveTree()
        tree = openTree(a, false)
        treeMutable = tree!!.mutableCopy as PatriciaTreeMutable
        treeMutable!!.delete(key("abbaa"))
        treeMutable!!.put(kv("abbab", "2"))
        treeMutable!!.put(kv("abbba", "5"))
        assertMatches(
            treeMutable!!,
            rm(
                "a",
                n('a', "a", "0"),
                nm(
                    'b',
                    "b",
                    nm('a', "b", "2"),
                    nm('b', "a", "5")
                ),
                n('c', "a", "3")
            )
        )
        a = saveTree()
        tree = openTree(a, false)
        treeMutable = tree!!.mutableCopy as PatriciaTreeMutable
        val loggables: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(0)
        treeMutable!!.reclaim(loggables.next(), loggables)
        assertMatches(
            treeMutable!!,
            rm(
                "a",
                nm('a', "a", "0"),
                n(
                    'b',
                    "b",
                    n('a', "b", "2"),
                    n('b', "a", "5")
                ),
                nm('c', "a", "3")
            )
        )
    }

    @Test
    fun testSplitBottom() {
        treeMutable = createMutableTree(false, 1)
        treeMutable!!.put(kv("aaab", "aaab"))
        treeMutable!!.put(kv("aaac", "aaac"))
        var a = saveTree()
        tree = openTree(a, false)
        treeMutable = tree!!.mutableCopy as PatriciaTreeMutable
        assertMatches(
            treeMutable!!,
            rm(
                "aaa",
                n('b', "", "aaab"),
                n('c', "", "aaac")
            )
        )
        treeMutable!!.put(kv("aabb", "aabb"))
        val secondAddress: Long = log!!.read(a).end()
        a = saveTree()
        tree = openTree(a, false)
        treeMutable = tree!!.mutableCopy as PatriciaTreeMutable
        var loggables: Iterator<RandomAccessLoggable> = log!!.getLoggableIterator(0)
        treeMutable!!.reclaim(loggables.next(), loggables)
        assertMatches(
            treeMutable!!,
            rm(
                "aa",
                nm(
                    'a',
                    nm('b', "", "aaab"),
                    nm('c', "", "aaac")
                ),
                n('b', "b", "aabb")
            )
        )
        treeMutable = tree!!.mutableCopy as PatriciaTreeMutable
        loggables = log!!.getLoggableIterator(secondAddress)
        treeMutable!!.reclaim(loggables.next(), loggables)
        assertMatches(
            treeMutable!!,
            rm(
                "aa",
                nm(
                    'a',
                    n('b', "", "aaab"),
                    n('c', "", "aaac")
                ),
                nm('b', "b", "aabb")
            )
        )
    }
}
