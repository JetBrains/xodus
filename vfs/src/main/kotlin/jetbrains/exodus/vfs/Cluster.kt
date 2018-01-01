/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.vfs

import jetbrains.exodus.*
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile

internal class Cluster(private val it: ByteIterable) : Iterator<Byte> {

    private var iterator: ByteIterator? = null
    var startingPosition: Long = 0
    var clusterNumber: Long = 0
    private var size: Int = 0

    fun getSize() = getIterator().run { size }

    override fun hasNext() = getSize() > 0

    override fun next() = getIterator().next().also { --size }

    fun skip(length: Long): Long {
        val size = getSize()
        val skipped = if (length > size) size.toLong() else getIterator().skip(length)
        this.size -= skipped.toInt()
        return skipped
    }

    fun copyTo(array: ByteArray) {
        var i = 0
        while (hasNext()) {
            array[i++] = next()
        }
    }

    private fun getIterator(): ByteIterator {
        return iterator ?: it.iterator().apply {
            iterator = this
            size = IntegerBinding.readCompressed(this)
        }
    }

    companion object {

        @JvmStatic
        fun writeCluster(cluster: ByteArray,
                         clusterConverter: ClusterConverter?,
                         size: Int,
                         accumulateInRAM: Boolean): ByteIterable {
            if (accumulateInRAM) {
                val output = LightOutputStream(size + 5)
                IntegerBinding.writeCompressed(output, size)
                output.write(cluster, 0, size)
                val result = output.asArrayByteIterable()
                return clusterConverter?.onWrite(result) ?: result
            }
            val bi: ByteIterable
            try {
                val file = File.createTempFile("~exodus-vfs-output-cluster", ".tmp")
                RandomAccessFile(file, "rw").use { out -> out.write(cluster, 0, size) }
                bi = FileByteIterable(file)
                file.deleteOnExit()
            } catch (e: IOException) {
                throw ExodusException.toExodusException(e)
            }

            return CompoundByteIterable(arrayOf(IntegerBinding.intToCompressedEntry(size), bi))
        }
    }
}