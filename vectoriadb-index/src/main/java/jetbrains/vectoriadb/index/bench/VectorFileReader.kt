/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.IndexBuilder
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.extension
import kotlin.math.min

interface VectorFileReader: AutoCloseable {
    fun read(idx: Int): FloatArray
    fun read(idx: Int, vector: FloatArray)
    fun readAll(): Array<FloatArray>

    fun readId(idx: Int): ByteArray {
        val idBuffer = ByteBuffer.allocate(IndexBuilder.VECTOR_ID_SIZE)
        idBuffer.order(ByteOrder.LITTLE_ENDIAN)
        idBuffer.putInt(idx)
        idBuffer.rewind()
        return idBuffer.array()
    }

    companion object {
        @JvmStatic
        fun openFileReader(filePath: Path, vectorDimensions: Int, maxVectorCount: Int = Int.MAX_VALUE): VectorFileReader {
            return when (filePath.extension) {
                "fvecs" -> FvecsFileReader(
                    FileChannel.open(filePath, StandardOpenOption.READ),
                    vectorDimensions,
                    maxVectorCount
                )

                "bvecs" -> BvecsFileReader(
                    FileChannel.open(filePath, StandardOpenOption.READ),
                    vectorDimensions,
                    maxVectorCount
                )
                else -> throw IllegalArgumentException("The file format ${filePath.fileName} is not supported")
            }
        }

        @JvmStatic
        fun wrapWithFileReader(channel: FileChannel, filePath: Path, vectorDimensions: Int, maxVectorCount: Int = Int.MAX_VALUE): VectorFileReader {
            return when (filePath.extension) {
                "fvecs" -> FvecsFileReader(channel, vectorDimensions, maxVectorCount)
                "bvecs" -> BvecsFileReader(channel, vectorDimensions, maxVectorCount)
                else -> throw IllegalArgumentException("The file format ${filePath.fileName} is not supported")
            }
        }
    }
}

class FvecsFileReader(
    private val channel: FileChannel,
    private val vectorDimensions: Int,
    private val maxVectorCount: Int = Int.MAX_VALUE
) : VectorFileReader {
    private val recordSize = Float.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES
    private val totalVectorsCount = (channel.size() / recordSize).toInt()
    private val buffer = ByteBuffer.allocate(recordSize)

    init {
        buffer.order(ByteOrder.LITTLE_ENDIAN)
    }

    override fun read(idx: Int): FloatArray {
        val vector = FloatArray(vectorDimensions)
        read(idx, vector)
        return vector
    }

    override fun read(idx: Int, vector: FloatArray) {
        while (buffer.hasRemaining()) {
            channel.read(buffer, recordSize.toLong() * idx + buffer.position())
        }
        buffer.rewind()

        check(buffer.getInt() == vectorDimensions) { "Vector dimensions mismatch" }

        for (i in vector.indices) {
            vector[i] = buffer.getFloat()
        }
        buffer.clear()
    }

    override fun readAll(): Array<FloatArray> {
        val vectorCount = min(maxVectorCount, totalVectorsCount)
        return Array(vectorCount) { i -> read(i) }
    }

    override fun close() {
        channel.close()
    }
}

class BvecsFileReader(
    private val channel: FileChannel,
    private val vectorDimensions: Int,
    private val maxVectorCount: Int = Int.MAX_VALUE
) : VectorFileReader {
    private val recordSize = vectorDimensions + Int.SIZE_BYTES
    private val totalVectorsCount = (channel.size() / recordSize).toInt()
    private val buffer = ByteBuffer.allocate(recordSize)

    init {
        buffer.order(ByteOrder.LITTLE_ENDIAN)
    }

    override fun read(idx: Int): FloatArray {
        val vector = FloatArray(vectorDimensions)
        read(idx, vector)
        return vector
    }

    override fun read(idx: Int, vector: FloatArray) {
        while (buffer.hasRemaining()) {
            channel.read(buffer, recordSize.toLong() * idx + buffer.position())
        }
        buffer.rewind()

        check(buffer.getInt() == vectorDimensions) { "Vector dimensions mismatch" }

        for (i in vector.indices) {
            vector[i] = buffer.get().toUByte().toFloat()
        }
        buffer.clear()
    }

    override fun readAll(): Array<FloatArray> {
        val vectorCount = min(maxVectorCount, totalVectorsCount)
        return Array(vectorCount) { i -> read(i) }
    }

    override fun close() {
        channel.close()
    }
}

fun readVectors(filePath: Path, vectorDimensions: Int, maxVectorCount: Int = Int.MAX_VALUE): Array<FloatArray> =
    VectorFileReader.openFileReader(filePath, vectorDimensions, maxVectorCount).use { fileReader ->
        fileReader.readAll()
    }