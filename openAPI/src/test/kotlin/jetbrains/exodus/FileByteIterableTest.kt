/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus

import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.FileOutputStream
import java.io.IOException

class FileByteIterableTest {

    private var file: File? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        file = File.createTempFile("FileByteIterable", null, TestUtil.createTempDir())
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        if (file!!.delete()) {
            val dir = file!!.parentFile
            if (!dir.delete()) {
                dir.deleteOnExit()
            }
        }
    }

    @Test
    @Throws(IOException::class)
    fun testEmptyIterable() {
        val it = FileByteIterable(file!!)
        Assert.assertEquals(0, compare(it.iterator(), ByteIterable.EMPTY_ITERATOR).toLong())
    }

    @Test
    @Throws(IOException::class)
    fun testSingleIterable() {
        FileOutputStream(file!!).use { output -> output.write(MANDELSTAM.toByteArray(charset("UTF-8"))) }
        val it = FileByteIterable(file!!)
        Assert.assertEquals(0, compare(it.iterator(), ArrayByteIterable(MANDELSTAM.toByteArray(charset("UTF-8"))).iterator()).toLong())
    }

    @Test
    @Throws(IOException::class)
    fun testMultipleIterables() {
        val count = 10
        FileOutputStream(file!!).use { output ->
            for (i in 0 until count) {
                output.write(MANDELSTAM.toByteArray(charset("UTF-8")))
            }
        }
        val sampleBytes = MANDELSTAM.toByteArray(charset("UTF-8"))
        val length = sampleBytes.size
        var i = 0
        var offset = 0
        while (i < count) {
            Assert.assertEquals(0, compare(FileByteIterable(file!!, offset.toLong(), length).iterator(), ArrayByteIterable(sampleBytes).iterator()).toLong())
            ++i
            offset += length
        }
    }

    companion object {

        private fun compare(i1: ByteIterator, i2: ByteIterator): Int {
            while (true) {
                val hasNext1 = i1.hasNext()
                val hasNext2 = i2.hasNext()
                if (!hasNext1) {
                    return if (hasNext2) -1 else 0
                }
                if (!hasNext2) {
                    return 1
                }
                val cmp = (i1.next().toInt() and 0xff) - (i2.next().toInt() and 0xff)
                if (cmp != 0) {
                    return cmp
                }
            }
        }
    }
}

private val MANDELSTAM = """"�? не ограблен я, и не надломлен,
                        "Но только что всего переогромлен.
                        "Как «Слово о Полку», струна моя туга,
                        "�? в голосе моем после удушья
                        "Звучит земля — последнее оружье —
                        "Сухая влажность черноземных га...""".trimIndent()